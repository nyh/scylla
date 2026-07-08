/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * This test measures how evenly the Seastar HTTP server distributes new
 * connections across shards.
 *
 * The server runs one httpd::http_server per shard and counts requests
 * received on each shard.  The client side opens many short-lived TCP
 * connections (one request per connection, then close) from every shard in
 * parallel.  At the end, the per-shard request counts are printed.
 *
 * A perfectly balanced server should show ~equal counts on every shard.
 * Due to the conntrack load-balancer bug (shard-0 decrement is applied
 * synchronously while other shards' decrements go through the cross-core
 * message queue), shard 0 typically receives significantly more connections
 * than the others.
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/http/client.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/short_streams.hh>
#include <fmt/format.h>
#include <boost/program_options.hpp>
#include <sys/resource.h>

using namespace seastar;
using namespace seastar::httpd;
namespace bpo = boost::program_options;

// -----------------------------------------------------------------------
// Per-shard request counter, incremented inside the HTTP handler.
// Being thread-local means it is always updated on the shard that actually
// processes the request, so the final value per shard tells us how many
// connections the conntrack load-balancer routed to that shard.
// -----------------------------------------------------------------------
thread_local uint64_t g_requests_on_shard = 0;
// Number of busy-loop iterations performed inside the request handler.
// Set once per shard during server_shard::start().
thread_local uint64_t g_handler_work = 0;
// Sleep duration injected by the request handler (0 = no sleep).
thread_local std::chrono::milliseconds g_handler_sleep{0};

// -----------------------------------------------------------------------
// Server side
// -----------------------------------------------------------------------

class server_shard : public seastar::peering_sharded_service<server_shard> {
    http_server _http_server;
public:
    server_shard() : _http_server("http_shard_balance") {}

    future<> start(uint16_t port, uint64_t handler_work, uint64_t handler_sleep_ms) {
        g_handler_work = handler_work;
        g_handler_sleep = std::chrono::milliseconds(handler_sleep_ms);
        _http_server._routes.put(
            GET, "/",
            new function_handler(
                [](std::unique_ptr<http::request>, std::unique_ptr<http::reply> rep) -> future<std::unique_ptr<http::reply>> {
                    ++g_requests_on_shard;
                    // Simulate CPU work in the handler.  volatile prevents
                    // the compiler from optimising the loop away.
                    volatile uint64_t dummy = 0;
                    for (uint64_t j = 0; j < g_handler_work; ++j) {
                        dummy += j;
                    }
                    if (g_handler_sleep.count() > 0) {
                        co_await sleep(g_handler_sleep);
                    }
                    rep->_content = "ok";
                    co_return std::move(rep);
                },
                "text/plain"));
        return _http_server.listen(
            socket_address(net::inet_address("127.0.0.1"), port));
    }

    future<> stop() {
        return _http_server.stop();
    }

    uint64_t request_count() const {
        return g_requests_on_shard;
    }
};

// -----------------------------------------------------------------------
// Client side
// -----------------------------------------------------------------------

class client_shard : public seastar::peering_sharded_service<client_shard> {
    uint16_t _port;
    uint64_t _requests_per_shard;
    unsigned _concurrency;
    uint64_t _errors = 0;

public:
    client_shard() = default;

    future<> start(uint16_t port, uint64_t requests_per_shard, unsigned concurrency) {
        _port = port;
        _requests_per_shard = requests_per_shard;
        _concurrency = concurrency;
        return make_ready_future<>();
    }

    future<> stop() {
        return make_ready_future<>();
    }

    // Send a single GET request over a brand-new TCP connection, then close it.
    // Creating a fresh http::client for every request forces a new TCP
    // connection, which exercises the conntrack accept path each time.
    future<> send_one_request() {
        http::client cli(socket_address(net::inet_address("127.0.0.1"), _port));
        std::exception_ptr ex;
        try {
            auto req = http::request::make("GET", "127.0.0.1", "/");
            // Ask the server to close the connection after this reply so that
            // the TCP connection is definitely torn down and the conntrack
            // counter is decremented promptly (or not, which is the bug).
            req._headers["Connection"] = "close";
            co_await cli.make_request(
                std::move(req),
                [](const http::reply&, input_stream<char>&& body) {
                    return do_with(std::move(body), [](auto& b) {
                        return util::skip_entire_stream(b).then([&b] {
                            return b.close();
                        });
                    });
                });
        } catch (...) {
            ex = std::current_exception();
            ++_errors;
        }
        co_await cli.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    // Keep up to _concurrency send_one_request() calls in flight at once.
    // A semaphore throttles dispatch; a gate lets us wait for all of them to
    // finish after the dispatch loop ends.
    future<> run() {
        semaphore limit(_concurrency);
        gate g;
        for (uint64_t i = 0; i < _requests_per_shard; ++i) {
            co_await limit.wait();
            // Fire-and-forget: send_one_request() already records errors in
            // _errors; we just swallow the (re-)throw here and release the
            // semaphore slot when done.
            (void)with_gate(g, [this, &limit]() -> future<> {
                return send_one_request()
                    .handle_exception([](std::exception_ptr) { /* already counted */ })
                    .finally([&limit] { limit.signal(); });
            });
        }
        co_await g.close();
    }

    uint64_t error_count() const { return _errors; }
};

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------

int main(int argc, char** argv) {
    // Raise the open-file-descriptor soft limit to the hard limit so that
    // high --concurrency values do not hit EMFILE ("Too many open files").
    // Each in-flight request uses two fds (client socket + accepted socket)
    // so with --smp N --concurrency C you need at least 2*N*C fds plus
    // Seastar's own per-shard overhead.
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            rl.rlim_cur = rl.rlim_max;
            setrlimit(RLIMIT_NOFILE, &rl);
        }
    }

    app_template app;
    app.add_options()
        ("port",
         bpo::value<uint16_t>()->default_value(10000),
         "TCP port for the HTTP server")
        ("requests-per-shard",
         bpo::value<uint64_t>()->default_value(10000),
         "number of single-request connections each client shard sends")
        ("concurrency",
         bpo::value<unsigned>()->default_value(100),
         "number of connections each client shard keeps in flight simultaneously")
        ("handler-work",
         bpo::value<uint64_t>()->default_value(0),
         "busy-loop iterations executed inside the request handler (0 = no extra work)")
        ("handler-sleep",
         bpo::value<uint64_t>()->default_value(0),
         "milliseconds to sleep inside the request handler via seastar::sleep (0 = no sleep)");

    return app.run(argc, argv, [&app]() -> future<> {
        const uint16_t port =
            app.configuration()["port"].as<uint16_t>();
        const uint64_t requests_per_shard =
            app.configuration()["requests-per-shard"].as<uint64_t>();
        const unsigned concurrency =
            app.configuration()["concurrency"].as<unsigned>();
        const uint64_t handler_work =
            app.configuration()["handler-work"].as<uint64_t>();
        const uint64_t handler_sleep_ms =
            app.configuration()["handler-sleep"].as<uint64_t>();

        fmt::print("Starting HTTP server on 127.0.0.1:{}\n", port);
        fmt::print("Each of {} shards will send {} single-request connections "
                   "({} total, {} in flight per shard, {} handler-work iters, {}ms handler-sleep)\n",
                   this_smp_shard_count(), requests_per_shard,
                   (uint64_t)this_smp_shard_count() * requests_per_shard,
                   concurrency, handler_work, handler_sleep_ms);

        sharded<server_shard> server;
        co_await server.start();
        co_await server.invoke_on_all(&server_shard::start, port, handler_work, handler_sleep_ms);

        // Give the server a moment to finish its listen setup on all shards
        co_await sleep(std::chrono::milliseconds(100));

        sharded<client_shard> clients;
        co_await clients.start();
        co_await clients.invoke_on_all(
            &client_shard::start, port, requests_per_shard, concurrency);

        fmt::print("Sending requests...\n");
        co_await clients.invoke_on_all(&client_shard::run);

        // ----------------------------------------------------------------
        // Collect and print results
        // ----------------------------------------------------------------
        fmt::print("\nResults: requests received per shard\n");
        fmt::print("{:<10} {:>12} {:>10}\n", "shard", "requests", "errors");
        fmt::print("{}\n", std::string(34, '-'));

        uint64_t total_requests = 0;
        uint64_t total_errors   = 0;

        for (unsigned s = 0; s < this_smp_shard_count(); ++s) {
            uint64_t reqs = co_await server.invoke_on(
                s, &server_shard::request_count);
            uint64_t errs = co_await clients.invoke_on(
                s, &client_shard::error_count);
            fmt::print("{:<10} {:>12} {:>10}\n", s, reqs, errs);
            total_requests += reqs;
            total_errors   += errs;
        }

        fmt::print("{}\n", std::string(34, '-'));
        fmt::print("{:<10} {:>12} {:>10}\n",
                   "total", total_requests, total_errors);

        if (this_smp_shard_count() > 1) {
            uint64_t expected = total_requests / this_smp_shard_count();
            uint64_t shard0 = co_await server.invoke_on(
                0, &server_shard::request_count);
            fmt::print("\nExpected per shard (ideal):  {}\n", expected);
            fmt::print("Shard 0 ratio vs ideal:      {:.2f}x\n",
                       expected > 0
                           ? (double)shard0 / (double)expected
                           : 0.0);
        }

        co_await clients.stop();
        co_await server.stop();
    });
}

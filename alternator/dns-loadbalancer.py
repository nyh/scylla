#!/usr/bin/python3
import dnslib.server
import dnslib
import random
import _thread
import urllib.request
import time

# The list of live nodes, all of them supposedly answering HTTP requests on
# alternator_port. One of these nodes will be returned at random from every
# DNS request. This list starts with one or more known nodes, but then the
# livenodes_update() thread periodically replaces this list by an up-to-date
# list retrieved from makeing a "localnodes" requests to one of these nodes.
livenodes = ['127.0.0.1']
alternator_port = 8000
def livenodes_update():
    global alternator_port
    global livenodes
    while True:
        # Contact one of the already known nodes by random, to fetch a new
        # list of known nodes.
        ip = random.choice(livenodes)
        url = 'http://{}:{}/localnodes'.format(ip, alternator_port)
        print('updating livenodes from {}'.format(url))
        try:
            nodes = urllib.request.urlopen(url, None, 1.0).read().decode('ascii')
            a = [x.strip('"').rstrip('"') for x in nodes.strip('[').rstrip(']').split(',')]
            # If we're successful, replace livenodes by the new list
            livenodes = a
            print(livenodes)
        except:
            # TODO: contacting this ip was unsuccessful, maybe we should
            # remove it from the list of live nodes.
            pass
        time.sleep(1)
_thread.start_new_thread(livenodes_update,())

class Resolver:
    def resolve(self, request, handler):
        qname = request.q.qname
        reply = request.reply()
        # Note responses have TTL 4, as in Amazon's Dynamo DNS
        ip = random.choice(livenodes)
        reply.add_answer(*dnslib.RR.fromZone('{} 4 A {}'.format(qname, ip)))
        return reply
resolver = Resolver()
logger = dnslib.server.DNSLogger(prefix=False)
server = dnslib.server.DNSServer(Resolver(), port=8053, address='localhost', logger=logger, tcp=True)
server.start_thread()

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print('Goodbye!')
finally:
    server.stop()

# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cassandra_tests.porting import *

from cassandra.protocol import FunctionFailure
from cassandra.util import Date, Time, Duration

from decimal import Decimal
from uuid import UUID
from datetime import datetime, timezone
from socket import getaddrinfo
import json

def testSelectJsonWithPagingWithFrozenTuple(cql, test_keyspace):
    uuid = UUID("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc")
    partitionKey = (uuid, 2)
    with create_table(cql, test_keyspace, "(k1 FROZEN<TUPLE<uuid, int>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))") as table:
        # prepare data
        for i in range(1, 5):
            execute(cql, table, "INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, (uuid, i), i)

        for pageSize in range(1, 6):
            # SELECT JSON
            assert_rows(execute_with_paging(cql, table, "SELECT JSON * FROM %s", pageSize),
                ["{\"k1\": [\"" + str(uuid) + "\", 2], \"c1\": [\"" + str(uuid) + "\", 1], \"value\": 1}"],
                ["{\"k1\": [\"" + str(uuid) + "\", 2], \"c1\": [\"" + str(uuid) + "\", 2], \"value\": 2}"],
                ["{\"k1\": [\"" + str(uuid) + "\", 2], \"c1\": [\"" + str(uuid) + "\", 3], \"value\": 3}"],
                ["{\"k1\": [\"" + str(uuid) + "\", 2], \"c1\": [\"" + str(uuid) + "\", 4], \"value\": 4}"])

            # SELECT toJson(column)
            assert_rows(execute_with_paging(cql, table, "SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                ["[\"" + str(uuid) + "\", 2]", "[\"" + str(uuid) + "\", 1]", "1"],
                ["[\"" + str(uuid) + "\", 2]", "[\"" + str(uuid) + "\", 2]", "2"],
                ["[\"" + str(uuid) + "\", 2]", "[\"" + str(uuid) + "\", 3]", "3"],
                ["[\"" + str(uuid) + "\", 2]", "[\"" + str(uuid) + "\", 4]", "4"])

def testSelectJsonWithPagingWithFrozenMap(cql, test_keyspace):
    uuid = UUID("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc")
    partitionKey = {1: (uuid, 1), 2: (uuid, 2)}
    with create_table(cql, test_keyspace, "(k1 FROZEN<map<int, tuple<uuid, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))") as table:
        # prepare data
        for i in range(1, 5):
            execute(cql, table, "INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, (uuid, i), i)

        for pageSize in range(1, 6):
            # SELECT JSON
            assert_rows(execute_with_paging(cql, table, "SELECT JSON * FROM %s", pageSize),
                          ["{\"k1\": {\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}, \"c1\": [\"" + str(uuid) + "\", 1], \"value\": 1}"],
                          ["{\"k1\": {\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}, \"c1\": [\"" + str(uuid) + "\", 2], \"value\": 2}"],
                          ["{\"k1\": {\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}, \"c1\": [\"" + str(uuid) + "\", 3], \"value\": 3}"],
                          ["{\"k1\": {\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}, \"c1\": [\"" + str(uuid) + "\", 4], \"value\": 4}"])

            # SELECT toJson(column)
            assert_rows(execute_with_paging(cql, table, "SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          ["{\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}", "[\"" + str(uuid) + "\", 1]", "1"],
                          ["{\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}", "[\"" + str(uuid) + "\", 2]", "2"],
                          ["{\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}", "[\"" + str(uuid) + "\", 3]", "3"],
                          ["{\"1\": [\"" + str(uuid) + "\", 1], \"2\": [\"" + str(uuid) + "\", 2]}", "[\"" + str(uuid) + "\", 4]", "4"])

def testSelectJsonWithPagingWithFrozenSet(cql, test_keyspace):
    uuid = UUID("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc")
    partitionKey = {((1, 2), 1), ((2, 3), 2)}
    with create_table(cql, test_keyspace, "(k1 frozen<set<tuple<list<int>, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))") as table:
        # prepare data
        for i in range(1, 5):
            execute(cql, table, "INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, (uuid, i), i)

        for pageSize in range(1, 6):
            # SELECT JSON
            assert_rows(execute_with_paging(cql, table, "SELECT JSON * FROM %s", pageSize),
                          ["{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + str(uuid) + "\", 1], \"value\": 1}"],
                          ["{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + str(uuid) + "\", 2], \"value\": 2}"],
                          ["{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + str(uuid) + "\", 3], \"value\": 3}"],
                          ["{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + str(uuid) + "\", 4], \"value\": 4}"])

            # SELECT toJson(column)
            assert_rows(execute_with_paging(cql, table, "SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          ["[[[1, 2], 1], [[2, 3], 2]]", "[\"" + str(uuid) + "\", 1]", "1"],
                          ["[[[1, 2], 1], [[2, 3], 2]]", "[\"" + str(uuid) + "\", 2]", "2"],
                          ["[[[1, 2], 1], [[2, 3], 2]]", "[\"" + str(uuid) + "\", 3]", "3"],
                          ["[[[1, 2], 1], [[2, 3], 2]]", "[\"" + str(uuid) + "\", 4]", "4"])

def testSelectJsonWithPagingWithFrozenList(cql, test_keyspace):
    uuid = UUID("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc")
    partitionKey = [(uuid, 2), (uuid, 3)]
    with create_table(cql, test_keyspace, "(k1 frozen<list<tuple<uuid, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))") as table:
        # prepare data
        for i in range(1, 5):
            execute(cql, table, "INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, (uuid, i), i)

        for pageSize in range(1, 6):
            # SELECT JSON
            assert_rows(execute_with_paging(cql, table, "SELECT JSON * FROM %s", pageSize),
                      ["{\"k1\": [[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]], \"c1\": [\"" + str(uuid) + "\", 1], \"value\": 1}"],
                      ["{\"k1\": [[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]], \"c1\": [\"" + str(uuid) + "\", 2], \"value\": 2}"],
                      ["{\"k1\": [[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]], \"c1\": [\"" + str(uuid) + "\", 3], \"value\": 3}"],
                      ["{\"k1\": [[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]], \"c1\": [\"" + str(uuid) + "\", 4], \"value\": 4}"])

            # SELECT toJson(column)
            assert_rows(execute_with_paging(cql, table, "SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                      ["[[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]]", "[\"" + str(uuid) + "\", 1]", "1"],
                      ["[[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]]", "[\"" + str(uuid) + "\", 2]", "2"],
                      ["[[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]]", "[\"" + str(uuid) + "\", 3]", "3"],
                      ["[[\"" + str(uuid) + "\", 2], [\"" + str(uuid) + "\", 3]]", "[\"" + str(uuid) + "\", 4]", "4"])

def testSelectJsonWithPagingWithFrozenUDT(cql, test_keyspace):
    uuid = UUID("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc")
    abc_tuple = collections.namedtuple('abc_tuple', ['a', 'b', 'c'])
    partitionKey = abc_tuple(1, 2, ["1", "2"])
    with create_type(cql, test_keyspace, "(a int, b int, c list<text>)") as type_name:
        with create_table(cql, test_keyspace, f"(k1 frozen<{type_name}>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))") as table:
            # prepare data
            for i in range(1, 5):
                execute(cql, table, "INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, (uuid, i), i)

            for pageSize in range(1, 6):
                # SELECT JSON
                assert_rows(execute_with_paging(cql, table, "SELECT JSON * FROM %s", pageSize),
                          ["{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + str(uuid) + "\", 1], \"value\": 1}"],
                          ["{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + str(uuid) + "\", 2], \"value\": 2}"],
                          ["{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + str(uuid) + "\", 3], \"value\": 3}"],
                          ["{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + str(uuid) + "\", 4], \"value\": 4}"])

                # SELECT toJson(column)
                assert_rows(execute_with_paging(cql, table, "SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          ["{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + str(uuid) + "\", 1]", "1"],
                          ["{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + str(uuid) + "\", 2]", "2"],
                          ["{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + str(uuid) + "\", 3]", "3"],
                          ["{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + str(uuid) + "\", 4]", "4"])

# Reproduces issue #7911, #7912, #7914, #7915, #7944, #7954
@pytest.mark.xfail(reason="issues #7912, #7914, #7915, #7944, #7954")
def testFromJsonFct(cql, test_keyspace):
    abc_tuple = collections.namedtuple('abc_tuple', ['a', 'b', 'c'])
    with create_type(cql, test_keyspace, "(a int, b uuid, c set<text>)") as type_name:
        with create_table(cql, test_keyspace, "(" +
                "k int PRIMARY KEY, " +
                "asciival ascii, " +
                "bigintval bigint, " +
                "blobval blob, " +
                "booleanval boolean, " +
                "dateval date, " +
                "decimalval decimal, " +
                "doubleval double, " +
                "floatval float, " +
                "inetval inet, " +
                "intval int, " +
                "smallintval smallint, " +
                "textval text, " +
                "timeval time, " +
                "timestampval timestamp, " +
                "timeuuidval timeuuid, " +
                "tinyintval tinyint, " +
                "uuidval uuid," +
                "varcharval varchar, " +
                "varintval varint, " +
                "listval list<int>, " +
                "frozenlistval frozen<list<int>>, " +
                "setval set<uuid>, " +
                "frozensetval frozen<set<uuid>>, " +
                "mapval map<ascii, int>," +
                "frozenmapval frozen<map<ascii, int>>," +
                "tupleval frozen<tuple<int, ascii, uuid>>," +
                "udtval frozen<" + type_name + ">," +
                "durationval duration)") as table:
            # fromJson() can only be used when the receiver type is known
            # Cassandra and Scylla print different error messages - Cassandra
            # says "fromJson() cannot be used in the selection clause", Scylla
            # "fromJson() can only be called if receiver type is known".
            assert_invalid_message(cql, table, "fromJson()", "SELECT fromJson(asciival) FROM %s", 0, 0)

            # FIXME: the following tests need *Java* as a UDF language, while
            # Scylla uses Lua, so I didn't translate them.
            #String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$")
            #createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$")
            #assertInvalidMessage("Ambiguous call to function",
            #    "INSERT INTO %s (k, textval) VALUES (?, " + func1 + "(fromJson(?)))", 0, "123")

            # fails JSON parsing
            # Reproduces issue #7911:
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\u038E\u0394\u03B4\u03E0")

            # handle nulls
            # Reproduces issue #7912:
            execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, None)
            assert_rows(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0), [0, None])

            execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, None)
            assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), [0, None])

            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, None)
            assert_rows(execute(cql, table, "SELECT k, udtval FROM %s WHERE k = ?", 0), [0, None])

            # ================ ascii ================
            execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"")
            assert_rows(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0), [0, "ascii text"])

            execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"")
            assert_rows(execute(cql, table, "SELECT k, asciival FROM %s WHERE k = ?", 0), [0, "ascii \" text"])

            # reproduces issue #7911:
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"\\u1fff\\u2013\\u33B4\\u2014\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "123")

            # test that we can use fromJson() in other valid places in queries
            assert_rows(execute(cql, table, "SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), ["ascii \" text"])
            execute(cql, table, "UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0")
            execute(cql, table, "DELETE FROM %s WHERE k = fromJson(?)", "0")

            # ================ bigint ================
            execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123")
            assert_rows(execute(cql, table, "SELECT k, bigintval FROM %s WHERE k = ?", 0), [0, 123123123123])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"123123123123\"")
            assert_rows(execute(cql, table, "SELECT k, bigintval FROM %s WHERE k = ?", 0), [0, 123123123123])

            # overflow (Long.MAX_VALUE + 1)
            # Reproduces #7914
            assert_invalid_throw_message(cql, table, "Expected a bigint value, but got a", FunctionFailure,
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "9223372036854775808")

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.456")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"abc\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "[\"abc\"]")

            # ================ blob ================
            execute(cql, table, "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"")
            assert_rows(execute(cql, table, "SELECT k, blobval FROM %s WHERE k = ?", 0), [0, bytearray([0,0,0,1])])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"123\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x123\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "123")

            # ================ boolean ================
            execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true")
            assert_rows(execute(cql, table, "SELECT k, booleanval FROM %s WHERE k = ?", 0), [0, True])

            execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false")
            assert_rows(execute(cql, table, "SELECT k, booleanval FROM %s WHERE k = ?", 0), [0, False])

            # strings are also accepted
            # Reproduces issue #7915
            execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "\"false\"")
            assert_rows(execute(cql, table, "SELECT k, booleanval FROM %s WHERE k = ?", 0), [0, False])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "\"abc\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "123")

            # ================ date ================
            execute(cql, table, "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"")
            assert_rows(execute(cql, table, "SELECT k, dateval FROM %s WHERE k = ?", 0), [0, Date("1987-03-23")])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "123")
            assert_invalid_throw_message(cql, table, "Unable to coerce 'xyz' to a formatted date", FunctionFailure,
                "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"xyz\"")

            # ================ decimal ================
            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123")
            assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), [0, Decimal("123123.123123")])

            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123")
            assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), [0, Decimal("123123")])

            # accept strings for numbers that cannot be represented as doubles
            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"")
            assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), [0, Decimal("123123.123123")])

            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"")
            assert_rows(execute(cql, table, "SELECT k, decimalval FROM %s WHERE k = ?", 0), [0, Decimal("-1.23E-12")])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "true")

            # ================ double ================
            execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123")
            assert_rows(execute(cql, table, "SELECT k, doubleval FROM %s WHERE k = ?", 0), [0, 123123.123123])

            execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123")
            assert_rows(execute(cql, table, "SELECT k, doubleval FROM %s WHERE k = ?", 0), [0, 123123.0])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"123123\"")
            assert_rows(execute(cql, table, "SELECT k, doubleval FROM %s WHERE k = ?", 0), [0, 123123.0])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "true")

            # ================ float ================
            execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123")
            assert_rows(execute(cql, table, "SELECT k, floatval FROM %s WHERE k = ?", 0), [0, to_float(123123.123123)])

            execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123")
            assert_rows(execute(cql, table, "SELECT k, floatval FROM %s WHERE k = ?", 0), [0, to_float(123123.0)])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"123123.0\"")
            assert_rows(execute(cql, table, "SELECT k, floatval FROM %s WHERE k = ?", 0), [0, to_float(123123.0)])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "true")

            # ================ inet ================
            execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"")
            assert_rows(execute(cql, table, "SELECT k, inetval FROM %s WHERE k = ?", 0), [0, "127.0.0.1"])

            execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"")
            assert_rows(execute(cql, table, "SELECT k, inetval FROM %s WHERE k = ?", 0), [0, "::1"])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "123")

            # ================ int ================
            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123")
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), [0, 123123])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"123123\"")
            assert_rows(execute(cql, table, "SELECT k, intval FROM %s WHERE k = ?", 0), [0, 123123])

            # int overflow (2 ^ 32, or Integer.MAX_INT + 1)
            # Reproduces #7914
            assert_invalid_throw_message(cql, table, "Expected an int value, but got a", FunctionFailure,
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "2147483648")

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                    "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123.456")
            assert_invalid_throw(cql, table, FunctionFailure,
                    "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                    "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "true")

            # ================ smallint ================
            execute(cql, table, "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "32767")
            assert_rows(execute(cql, table, "SELECT k, smallintval FROM %s WHERE k = ?", 0), [0, 32767])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "\"32767\"")
            assert_rows(execute(cql, table, "SELECT k, smallintval FROM %s WHERE k = ?", 0), [0, 32767])

            # smallint overflow (Short.MAX_VALUE + 1)
            # Reproduces #7914
            assert_invalid_throw_message(cql, table, "Unable to make short from", FunctionFailure,
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "32768")

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "123.456")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "true")

            # ================ tinyint ================
            execute(cql, table, "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "127")
            assert_rows(execute(cql, table, "SELECT k, tinyintval FROM %s WHERE k = ?", 0), [0, 127])

            # strings are also accepted
            execute(cql, table, "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "\"127\"")
            assert_rows(execute(cql, table, "SELECT k, tinyintval FROM %s WHERE k = ?", 0), [0, 127])

            # tinyint overflow (Byte.MAX_VALUE + 1)
            # Reproduces #7914
            assert_invalid_throw_message(cql, table, "Unable to make byte from", FunctionFailure,
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "128")

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "123.456")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "true")

            # ================ text (varchar) ================
            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\"")
            assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), [0, ""])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"")
            assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), [0, "abcd"])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"")
            assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), [0, "some \" text"])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"")
            assert_rows(execute(cql, table, "SELECT k, textval FROM %s WHERE k = ?", 0), [0, "\u2013"])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "123")

            # ================ time ================
            execute(cql, table, "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"")
            assert_rows(execute(cql, table, "SELECT k, timeval FROM %s WHERE k = ?", 0), [0, Time("07:35:07.000111222")])

            # Reproduces #7911
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "123456")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"xyz\"")

            # ================ timestamp ================
            execute(cql, table, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123")
            assert_rows(execute(cql, table, "SELECT k, timestampval FROM %s WHERE k = ?", 0), [0, datetime.utcfromtimestamp(123123123123/1e3)])

            execute(cql, table, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"")
            # The following comparison is a big mess in Python because of
            # timezone issues. We need the datetime() object to have a UTC
            # timezone, but not to indicate that it does otherwise the
            # result will not compare equal. The following weird conversion
            # appears to do the right thing...
            assert_rows(execute(cql, table, "SELECT k, timestampval FROM %s WHERE k = ?", 0), [0, datetime.utcfromtimestamp(datetime(2014, 1, 1, 0, 0, 0).timestamp())])
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123.456")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"abcd\"")

            # ================ timeuuid ================
            execute(cql, table, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"")
            assert_rows(execute(cql, table, "SELECT k, timeuuidval FROM %s WHERE k = ?", 0), [0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")])

            execute(cql, table, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"")
            assert_rows(execute(cql, table, "SELECT k, timeuuidval FROM %s WHERE k = ?", 0), [0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-0000-000000000000\"")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "123")

             # ================ uuidval ================
            execute(cql, table, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"")
            assert_rows(execute(cql, table, "SELECT k, uuidval FROM %s WHERE k = ?", 0), [0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")])

            execute(cql, table, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"")
            assert_rows(execute(cql, table, "SELECT k, uuidval FROM %s WHERE k = ?", 0), [0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799")])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-zzzz-000000000000\"")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "123")

            # ================ varint ================
            execute(cql, table, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123")
            assert_rows(execute(cql, table, "SELECT k, varintval FROM %s WHERE k = ?", 0), [0, 123123123123])

            # accept strings for numbers that cannot be represented as longs
            execute(cql, table, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"")
            assert_rows(execute(cql, table, "SELECT k, varintval FROM %s WHERE k = ?", 0), [0, 1234567890123456789012345678901234567890])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123.123")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"")

            # reproduces #7944
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"\"")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "true")

            # ================ lists ================
            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]")
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), [0, [1, 2, 3]])

            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]")
            assert_rows(execute(cql, table, "SELECT k, listval FROM %s WHERE k = ?", 0), [0, None])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "123")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[\"abc\"]")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[null]")

            # frozen
            execute(cql, table, "INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]")
            assert_rows(execute(cql, table, "SELECT k, frozenlistval FROM %s WHERE k = ?", 0), [0, [1, 2, 3]])

            # ================ sets ================
            execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
            assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                [0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))}])

            # duplicates are okay, just like in CQL
            execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
            assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0),
                [0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))}])

            execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]")
            assert_rows(execute(cql, table, "SELECT k, setval FROM %s WHERE k = ?", 0), [0, None])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "123")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[\"abc\"]")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[null]")

            # frozen
            execute(cql, table, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
            assert_rows(execute(cql, table, "SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                [0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))}])

            execute(cql, table, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]")
            assert_rows(execute(cql, table, "SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                [0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))}])

            # ================ maps ================
            # Reproduces #7949:
            execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}")
            assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0), [0, {"a": 1, "b": 2}])

            execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}")
            assert_rows(execute(cql, table, "SELECT k, mapval FROM %s WHERE k = ?", 0), [0, None])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "123")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"\\u1fff\\u2013\\u33B4\\u2014\": 1}")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": null}")

            # frozen
            # Reproduces #7949:
            execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}")
            assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), [0, {"a": 1, "b": 2}])
            execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 1}")
            assert_rows(execute(cql, table, "SELECT k, frozenmapval FROM %s WHERE k = ?", 0), [0, {"a": 1, "b": 2}])

            # ================ tuples ================
            execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
            assert_rows(execute(cql, table, "SELECT k, tupleval FROM %s WHERE k = ?", 0),
                [0, (1, "foobar", UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))])

            # Reproduces #7954:
            execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
            assert_rows(execute(cql, table, "SELECT k, tupleval FROM %s WHERE k = ?", 0),
                [0, (1, None, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\", 1, 2, 3]")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\"]")

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[\"not an int\", \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")

            # ================ UDTs ================
            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}")
            assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                [0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}])

            # ================ duration ================
            execute(cql, table, "INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"53us\"")
            assert_rows(execute(cql, table, "SELECT k, durationval FROM %s WHERE k = ?", 0), [0, Duration(0, 0, 53000)])

            execute(cql, table, "INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"P2W\"")
            assert_rows(execute(cql, table,"SELECT k, durationval FROM %s WHERE k = ?", 0), [0, Duration(0, 14, 0)])

            # Unlike all the other cases of unsuccessful fromJson() parsing which return FunctionFailure,
            # in this specific case Cassandra returns InvalidQuery. I don't know why, and I don't think
            # Scylla needs to reproduce this ideosyncracy. So let's allow both.
            assert_invalid_throw(cql, table, (FunctionFailure, InvalidRequest),
                "INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"xyz\"")

            # order of fields shouldn't matter
            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}")
            assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                [0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}])

            # test nulls
            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}")
            assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                [0, None, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"bar", "foo"}])

            # test missing fields
            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}")
            assert_rows(execute(cql, table, "SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                [0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), None])

            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"xxx\": 1}")
            assert_invalid_throw(cql, table, FunctionFailure,
                "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": \"foobar\"}")

# The following test will check the output of Cassandra's and Scylla's toJson()
# function, which converts various types to JSON. However, obviously there is
# more than one correct way to format the same JSON object, so in many cases
# we cannot, and don't want to, expect the exact same string to be returned by
# Scylla and Cassandra. For this we have the following class. It wraps
# a JSON string, and compare equal to other strings if both are valid JSON
# strings which decode to the same object. EquivalentJson("....") can be used
# in assert_rows() checks below, to check whether functionally-equivalent JSON
# is returned instead of checking for identical strings.
class EquivalentJson:
    def __init__(self, s):
        self.obj = json.loads(s)
    def __eq__(self, other):
        if isinstance(other, EquivalentJson):
            return self.obj == other.obj
        elif isinstance(other, str):
            return self.obj == json.loads(other)
        return NotImplemented
    # Implementing __repr__ is useful because when a comparison fails, pytest
    # helpfully prints what it tried to compare, and uses __repr__ for that.
    def __repr__(self):
        return f'EquivalentJson("{self.obj}")'

# Similarly, EquivalentIp compares two JSON strings which are supposed to
# contain an IP address. For example, "::1" and "0:0:0:0:0:0:0:1" are
# equivalent.
class EquivalentIp:
    def __init__(self, s):
        self.obj = json.loads(s)
    def __eq__(self, other):
        if isinstance(other, EquivalentIp):
            otherobj = other.obj
        elif isinstance(other, str):
            otherobj = json.loads(other)
        else:
            return NotImplemented
        if self.obj == otherobj:
            return True
        return getaddrinfo(self.obj, 0) == getaddrinfo(otherobj, 0)
    def __repr__(self):
        return f'EquivalentIp("{self.obj}")'

# Reproduces issue #7972, #7988, #7997, #8001
@pytest.mark.xfail(reason="issues #7972, #7988, #7997, #8001")
def testToJsonFct(cql, test_keyspace):
    abc_tuple = collections.namedtuple('abc_tuple', ['a', 'b', 'c'])
    with create_type(cql, test_keyspace, "(a int, b uuid, c set<text>)") as type_name:
        with create_table(cql, test_keyspace, "(" +
                "k int PRIMARY KEY, " +
                "asciival ascii, " +
                "bigintval bigint, " +
                "blobval blob, " +
                "booleanval boolean, " +
                "dateval date, " +
                "decimalval decimal, " +
                "doubleval double, " +
                "floatval float, " +
                "inetval inet, " +
                "intval int, " +
                "smallintval smallint, " +
                "textval text, " +
                "timeval time, " +
                "timestampval timestamp, " +
                "timeuuidval timeuuid, " +
                "tinyintval tinyint, " +
                "uuidval uuid," +
                "varcharval varchar, " +
                "varintval varint, " +
                "listval list<int>, " +
                "frozenlistval frozen<list<int>>, " +
                "setval set<uuid>, " +
                "frozensetval frozen<set<uuid>>, " +
                "mapval map<ascii, int>," +
                "frozenmapval frozen<map<ascii, int>>," +
                "tupleval frozen<tuple<int, ascii, uuid>>," +
                "udtval frozen<" + type_name + ">," +
                "durationval duration)") as table:
            # toJson() can only be used in selections
            # The error message is slightly different in Cassandra and in
            # Scylla. It is "toJson() may only be used within the selection
            # clause" in Cassandra, "toJson() is only valid in SELECT clause"
            # in Scylla.
            assert_invalid_message(cql, table, "clause",
                "INSERT INTO %s (k, asciival) VALUES (?, toJson(?))", 0, 0)
            assert_invalid_message(cql, table, "clause",
                "UPDATE %s SET asciival = toJson(?) WHERE k = ?", 0, 0)
            assert_invalid_message(cql, table, "clause",
                "DELETE FROM %s WHERE k = fromJson(toJson(?))", 0)

            # ================ ascii ================
            execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "ascii text")
            assert_rows(execute(cql, table, "SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), [0, "\"ascii text\""])

            execute(cql, table, "INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "")
            assert_rows(execute(cql, table, "SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), [0, "\"\""])

            # ================ bigint ================
            execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 123123123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), [0, "123123123123"])

            execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), [0, "0"])

            execute(cql, table, "INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, -123123123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), [0, "-123123123123"])

            # ================ blob ================
            execute(cql, table, "INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, bytearray([0,0,0,1]))
            assert_rows(execute(cql, table, "SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), [0, "\"0x00000001\""])

            execute(cql, table, "INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, bytearray())
            assert_rows(execute(cql, table, "SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), [0, "\"0x\""])

            # ================ boolean ================
            execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, True)
            assert_rows(execute(cql, table, "SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), [0, "true"])

            execute(cql, table, "INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, False)
            assert_rows(execute(cql, table, "SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), [0, "false"])

            # ================ date ================
            execute(cql, table, "INSERT INTO %s (k, dateval) VALUES (?, ?)", 0, Date("1987-03-23"))
            assert_rows(execute(cql, table, "SELECT k, toJson(dateval) FROM %s WHERE k = ?", 0), [0, "\"1987-03-23\""])

            # ================ decimal ================
            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, Decimal("123123.123123"))
            assert_rows(execute(cql, table, "SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), [0, "123123.123123"])

            execute(cql, table, "INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, Decimal("-1.23E-12"))
            # Scylla may print floating-point numbers with different choice
            # of capitalization, exponent, etc, so we use EquivalentJson.
            # Note that some representations may be equivalent, but
            # objectively bad - see issue #80002.
            assert_rows(execute(cql, table, "SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), [0, EquivalentJson("-1.23E-12")])

            # ================ double ================
            # Reproduces #7972:
            execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123.123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), [0, "123123.123123"])
            execute(cql, table, "INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), [0, "123123.0"])

            # ================ float ================
            execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123.123)
            assert_rows(execute(cql, table, "SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), [0, "123.123"])

            execute(cql, table, "INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123123)
            # Cassandra prints "123123.0", Scylla prints "123123". Since JSON
            # does not have a distinction between integers and floating point,
            # this difference is fine.
            assert_rows(execute(cql, table, "SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), [0, EquivalentJson("123123.0")])

            # ================ inet ================
            execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, "127.0.0.1")
            assert_rows(execute(cql, table, "SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), [0, "\"127.0.0.1\""])

            execute(cql, table, "INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, "::1")
            # Cassandra prints ::1 as "0:0:0:0:0:0:0:1", Scylla as "::1", both
            # are fine... We need to compare them using IP address equivalence
            # test...
            assert_rows(execute(cql, table, "SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), [0, EquivalentIp("\"0:0:0:0:0:0:0:1\"")])

            # ================ int ================
            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), [0, "123123"])

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), [0, "0"])

            execute(cql, table, "INSERT INTO %s (k, intval) VALUES (?, ?)", 0, -123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), [0, "-123123"])

            # ================ smallint ================
            execute(cql, table, "INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, 32767)
            assert_rows(execute(cql, table, "SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), [0, "32767"])

            execute(cql, table, "INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), [0, "0"])

            execute(cql, table, "INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, -32768)
            assert_rows(execute(cql, table, "SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), [0, "-32768"])

            # ================ tinyint ================
            execute(cql, table, "INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, 127)
            assert_rows(execute(cql, table, "SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), [0, "127"])

            execute(cql, table, "INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), [0, "0"])

            execute(cql, table, "INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, -128)
            assert_rows(execute(cql, table, "SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), [0, "-128"])

            # ================ text (varchar) ================
            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "")
            assert_rows(execute(cql, table, "SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), [0, "\"\""])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "abcd")
            assert_rows(execute(cql, table, "SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), [0, "\"abcd\""])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "\u8422")
            assert_rows(execute(cql, table, "SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), [0, "\"\u8422\""])

            execute(cql, table, "INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "\u0000")
            assert_rows(execute(cql, table, "SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), [0, "\"\\u0000\""])

            # ================ time ================
            # Reproduces #7988:
            execute(cql, table, "INSERT INTO %s (k, timeval) VALUES (?, ?)", 0, 123)
            assert_rows(execute(cql, table, "SELECT k, toJson(timeval) FROM %s WHERE k = ?", 0), [0, "\"00:00:00.000000123\""])
            execute(cql, table, "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"")
            assert_rows(execute(cql, table, "SELECT k, toJson(timeval) FROM %s WHERE k = ?", 0), [0, "\"07:35:07.000111222\""])

            # ================ timestamp ================
            # Reproduces #7997:
            execute(cql, table, "INSERT INTO %s (k, timestampval) VALUES (?, ?)", 0, datetime(2014, 1, 1, 0, 0, 0))
            assert_rows(execute(cql, table, "SELECT k, toJson(timestampval) FROM %s WHERE k = ?", 0), [0, "\"2014-01-01 00:00:00.000Z\""])

            # ================ timeuuid ================
            execute(cql, table, "INSERT INTO %s (k, timeuuidval) VALUES (?, ?)", 0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))
            assert_rows(execute(cql, table, "SELECT k, toJson(timeuuidval) FROM %s WHERE k = ?", 0), [0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""])

            # ================ uuidval ================
            execute(cql, table, "INSERT INTO %s (k, uuidval) VALUES (?, ?)", 0, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))
            assert_rows(execute(cql, table, "SELECT k, toJson(uuidval) FROM %s WHERE k = ?", 0), [0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""])

            # ================ varint ================
            execute(cql, table, "INSERT INTO %s (k, varintval) VALUES (?, ?)", 0, 123123123123123123123)
            assert_rows(execute(cql, table, "SELECT k, toJson(varintval) FROM %s WHERE k = ?", 0), [0, "123123123123123123123"])

            # ================ lists ================
            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, ?)", 0, [1, 2, 3])
            assert_rows(execute(cql, table, "SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), [0, "[1, 2, 3]"])

            execute(cql, table, "INSERT INTO %s (k, listval) VALUES (?, ?)", 0, [])
            assert_rows(execute(cql, table, "SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), [0, "null"])

            # frozen
            execute(cql, table, "INSERT INTO %s (k, frozenlistval) VALUES (?, ?)", 0, [1, 2, 3])
            assert_rows(execute(cql, table, "SELECT k, toJson(frozenlistval) FROM %s WHERE k = ?", 0), [0, "[1, 2, 3]"])

            # ================ sets ================
            execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, ?)",
                0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})
            assert_rows(execute(cql, table, "SELECT k, toJson(setval) FROM %s WHERE k = ?", 0),
                [0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]"])

            execute(cql, table, "INSERT INTO %s (k, setval) VALUES (?, ?)", 0, {})
            assert_rows(execute(cql, table, "SELECT k, toJson(setval) FROM %s WHERE k = ?", 0), [0, "null"])

            # frozen
            execute(cql, table, "INSERT INTO %s (k, frozensetval) VALUES (?, ?)",
                0, {UUID("6bddc89a-5644-11e4-97fc-56847afe9798"), UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})
            assert_rows(execute(cql, table, "SELECT k, toJson(frozensetval) FROM %s WHERE k = ?", 0),
                [0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]"])

            # ================ maps ================
            execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, {"a": 1, "b": 2})
            assert_rows(execute(cql, table, "SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), [0, "{\"a\": 1, \"b\": 2}"])

            execute(cql, table, "INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, {})
            assert_rows(execute(cql, table, "SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), [0, "null"])

            # frozen
            execute(cql, table, "INSERT INTO %s (k, frozenmapval) VALUES (?, ?)", 0, {"a": 1, "b": 2})
            assert_rows(execute(cql, table, "SELECT k, toJson(frozenmapval) FROM %s WHERE k = ?", 0), [0, "{\"a\": 1, \"b\": 2}"])

            # ================ tuples ================
            execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, (1, "foobar", UUID("6bddc89a-5644-11e4-97fc-56847afe9799")))
            assert_rows(execute(cql, table, "SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
                [0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]"])

            execute(cql, table, "INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, (1, "foobar", None))
            assert_rows(execute(cql, table, "SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
                [0, "[1, \"foobar\", null]"])

            # ================ UDTs ================
            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?, c: ?})", 0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"), {"foo", "bar"})
            assert_rows(execute(cql, table, "SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                [0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"bar\", \"foo\"]}"])

            execute(cql, table, "INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?})", 0, 1, UUID("6bddc89a-5644-11e4-97fc-56847afe9799"))
            assert_rows(execute(cql, table, "SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                [0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": null}"])

            # ================ duration ================
            # Reproduces #8001:
            execute(cql, table, "INSERT INTO %s (k, durationval) VALUES (?, 12µs)", 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(durationval) FROM %s WHERE k = ?", 0), [0, "\"12us\""])

            execute(cql, table, "INSERT INTO %s (k, durationval) VALUES (?, P1Y1M2DT10H5M)", 0)
            assert_rows(execute(cql, table, "SELECT k, toJson(durationval) FROM %s WHERE k = ?", 0), [0, "\"1y1mo2d10h5m\""])

"""
    @Test
    public void testJsonWithGroupBy() throws Throwable
    {
        // tests SELECT JSON statements
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 1)");

        assertRows(execute("SELECT JSON * FROM %s GROUP BY k"),
                   row("{\"k\": 0, \"c\": 0, \"v\": 0}"),
                   row("{\"k\": 1, \"c\": 0, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON k, c, v FROM %s GROUP BY k"),
                   row("{\"k\": 0, \"c\": 0, \"v\": 0}"),
                   row("{\"k\": 1, \"c\": 0, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON count(*) FROM %s GROUP BY k"),
                row("{\"count\": 2}"),
                row("{\"count\": 1}")
        );
    }

    @Test
    public void testSelectJsonSyntax() throws Throwable
    {
        // tests SELECT JSON statements
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");

        assertRows(execute("SELECT JSON * FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON k, v FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON v, k FROM %s"),
                row("{\"v\": 0, \"k\": 0}"),
                row("{\"v\": 1, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON v as foo, k as bar FROM %s"),
                row("{\"foo\": 0, \"bar\": 0}"),
                row("{\"foo\": 1, \"bar\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v), k FROM %s"),
                row("{\"ttl(v)\": null, \"k\": 0}"),
                row("{\"ttl(v)\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v) as foo, k FROM %s"),
                row("{\"foo\": null, \"k\": 0}"),
                row("{\"foo\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON count(*) FROM %s"),
                row("{\"count\": 2}")
        );

        assertRows(execute("SELECT JSON count(*) as foo FROM %s"),
                row("{\"foo\": 2}")
        );

        assertRows(execute("SELECT JSON toJson(blobAsInt(intAsBlob(v))) FROM %s LIMIT 1"),
                row("{\"system.tojson(system.blobasint(system.intasblob(v)))\": \"0\"}")
        );
    }

    @Test
    public void testInsertJsonSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0)
        );

        // without specifying column names
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0)
        );

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": null}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        execute("INSERT INTO %s JSON ?", "{\"v\": 1, \"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 1)
        );

        execute("INSERT INTO %s JSON ?", "{\"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        if (USE_PREPARED_VALUES)
            assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s JSON ?", new Object[]{null});

        assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s JSON ?", "null");
        assertInvalidMessage("Could not decode JSON string as a map", "INSERT INTO %s JSON ?", "\"notamap\"");
        assertInvalidMessage("Could not decode JSON string as a map", "INSERT INTO %s JSON ?", "12.34");
        assertInvalidMessage("JSON values map contains unrecognized column",
                "INSERT INTO %s JSON ?",
                "{\"k\": 0, \"v\": 0, \"zzz\": 0}");

        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s JSON ?",
                "{\"k\": 0, \"v\": \"notanint\"}");
    }

    @Test
    public void testInsertJsonSyntaxDefaultUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v1\": 0, \"v2\": 0}");

        // leave v1 unset
        execute("INSERT INTO %s JSON ? DEFAULT UNSET", "{\"k\": 0, \"v2\": 2}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0, 2)
        );

        // explicit specification DEFAULT NULL
        execute("INSERT INTO %s JSON ? DEFAULT NULL", "{\"k\": 0, \"v2\": 2}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null, 2)
        );

        // implicitly setting v2 to null
        execute("INSERT INTO %s JSON ? DEFAULT NULL", "{\"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null, null)
        );

        // mix setting null explicitly with default unset:
        // set values for all fields
        execute("INSERT INTO %s JSON ?", "{\"k\": 1, \"v1\": 1, \"v2\": 1}");
        // explicitly set v1 to null while leaving v2 unset which retains its value
        execute("INSERT INTO %s JSON ? DEFAULT UNSET", "{\"k\": 1, \"v1\": null}");
        assertRows(execute("SELECT * FROM %s WHERE k=1"),
                row(1, null, 1)
        );

        // test string literal instead of bind marker
        execute("INSERT INTO %s JSON '{\"k\": 2, \"v1\": 2, \"v2\": 2}'");
        // explicitly set v1 to null while leaving v2 unset which retains its value
        execute("INSERT INTO %s JSON '{\"k\": 2, \"v1\": null}' DEFAULT UNSET");
        assertRows(execute("SELECT * FROM %s WHERE k=2"),
                row(2, null, 2)
        );
        execute("INSERT INTO %s JSON '{\"k\": 2}' DEFAULT NULL");
        assertRows(execute("SELECT * FROM %s WHERE k=2"),
                row(2, null, null)
        );
    }

    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, \"Foo\" int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"\\\"Foo\\\"\": 0}");
        execute("INSERT INTO %s JSON ?", "{\"K\": 0, \"\\\"Foo\\\"\": 0}");
        execute("INSERT INTO %s JSON ?", "{\"\\\"k\\\"\": 0, \"\\\"Foo\\\"\": 0}");

        // results should preserve and quote case-sensitive identifiers
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"\\\"Foo\\\"\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as foo FROM %s"), row("{\"k\": 0, \"foo\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as \"Bar\" FROM %s"), row("{\"k\": 0, \"\\\"Bar\\\"\": 0}"));

        assertInvalid("INSERT INTO %s JSON ?", "{\"k\": 0, \"foo\": 0}");
        assertInvalid("INSERT INTO %s JSON ?", "{\"k\": 0, \"\\\"foo\\\"\": 0}");

        // user-defined types also need to handle case-sensitivity
        String typeName = createType("CREATE TYPE %s (a int, \"Foo\" int)");
        createTable("CREATE TABLE %s (k int primary key, v frozen<" + typeName + ">)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}");
        assertRows(execute("SELECT JSON k, v FROM %s"), row("{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}"));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": {\"A\": 0, \"\\\"Foo\\\"\": 0}}");
        assertRows(execute("SELECT JSON k, v FROM %s"), row("{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}"));
    }

    @Test
    public void testInsertJsonSyntaxWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "m map<text, boolean>, " +
                "mf frozen<map<text, boolean>>, " +
                "s set<int>, " +
                "sf frozen<set<int>>, " +
                "l list<int>, " +
                "lf frozen<list<int>>)");

        // map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"m\": {\"a\": true, \"b\": false}}");
        assertRows(execute("SELECT k, m FROM %s"), row(0, map("a", true, "b", false)));

        // frozen map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"mf\": {\"a\": true, \"b\": false}}");
        assertRows(execute("SELECT k, mf FROM %s"), row(0, map("a", true, "b", false)));

        // set
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"s\": [3, 1, 2]}");
        assertRows(execute("SELECT k, s FROM %s"), row(0, set(1, 2, 3)));

        // frozen set
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"sf\": [3, 1, 2]}");
        assertRows(execute("SELECT k, sf FROM %s"), row(0, set(1, 2, 3)));

        // list
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"l\": [1, 2, 3]}");
        assertRows(execute("SELECT k, l FROM %s"), row(0, list(1, 2, 3)));

        // frozen list
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"lf\": [1, 2, 3]}");
        assertRows(execute("SELECT k, lf FROM %s"), row(0, list(1, 2, 3)));
    }

    @Test
    public void testInsertJsonSyntaxWithNonNativeMapKeys() throws Throwable
    {
        // JSON doesn't allow non-string keys, so we accept string representations of any type as map keys and
        // return maps with string keys when necessary.

        String typeName = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "intmap map<int, boolean>, " +
                "bigintmap map<bigint, boolean>, " +
                "varintmap map<varint, boolean>, " +
                "smallintmap map<smallint, boolean>, " +
                "tinyintmap map<tinyint, boolean>, " +
                "booleanmap map<boolean, boolean>, " +
                "floatmap map<float, boolean>, " +
                "doublemap map<double, boolean>, " +
                "decimalmap map<decimal, boolean>, " +
                "tuplemap map<frozen<tuple<int, text>>, boolean>, " +
                "udtmap map<frozen<" + typeName + ">, boolean>, " +
                "setmap map<frozen<set<int>>, boolean>, " +
                "listmap map<frozen<list<int>>, boolean>, " +
                "textsetmap map<frozen<set<text>>, boolean>, " +
                "nestedsetmap map<frozen<map<set<text>, text>>, boolean>, " +
                "frozensetmap frozen<map<set<int>, boolean>>)");

        // int keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"intmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, intmap FROM %s"), row("{\"k\": 0, \"intmap\": {\"0\": true, \"1\": false}}"));

        // bigint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"bigintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, bigintmap FROM %s"), row("{\"k\": 0, \"bigintmap\": {\"0\": true, \"1\": false}}"));

        // varint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"varintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, varintmap FROM %s"), row("{\"k\": 0, \"varintmap\": {\"0\": true, \"1\": false}}"));

        // smallint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"smallintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, smallintmap FROM %s"), row("{\"k\": 0, \"smallintmap\": {\"0\": true, \"1\": false}}"));

        // tinyint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"tinyintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, tinyintmap FROM %s"), row("{\"k\": 0, \"tinyintmap\": {\"0\": true, \"1\": false}}"));

        // boolean keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"booleanmap\": {\"true\": true, \"false\": false}}");
        assertRows(execute("SELECT JSON k, booleanmap FROM %s"), row("{\"k\": 0, \"booleanmap\": {\"false\": false, \"true\": true}}"));

        // float keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"floatmap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, floatmap FROM %s"), row("{\"k\": 0, \"floatmap\": {\"1.23\": true, \"4.56\": false}}"));

        // double keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"doublemap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, doublemap FROM %s"), row("{\"k\": 0, \"doublemap\": {\"1.23\": true, \"4.56\": false}}"));

        // decimal keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"decimalmap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, decimalmap FROM %s"), row("{\"k\": 0, \"decimalmap\": {\"1.23\": true, \"4.56\": false}}"));

        // tuple<int, text> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"tuplemap\": {\"[0, \\\"a\\\"]\": true, \"[1, \\\"b\\\"]\": false}}");
        assertRows(execute("SELECT JSON k, tuplemap FROM %s"), row("{\"k\": 0, \"tuplemap\": {\"[0, \\\"a\\\"]\": true, \"[1, \\\"b\\\"]\": false}}"));

        // UDT keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"udtmap\": {\"{\\\"a\\\": 0}\": true, \"{\\\"a\\\": 1}\": false}}");
        assertRows(execute("SELECT JSON k, udtmap FROM %s"), row("{\"k\": 0, \"udtmap\": {\"{\\\"a\\\": 0}\": true, \"{\\\"a\\\": 1}\": false}}"));

        // set<int> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"setmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, setmap FROM %s"), row("{\"k\": 0, \"setmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));

        // list<int> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"listmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, listmap FROM %s"), row("{\"k\": 0, \"listmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));

        // set<text> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"textsetmap\": {\"[\\\"0\\\", \\\"1\\\"]\": true, \"[\\\"3\\\", \\\"4\\\"]\": false}}");
        assertRows(execute("SELECT JSON k, textsetmap FROM %s"), row("{\"k\": 0, \"textsetmap\": {\"[\\\"0\\\", \\\"1\\\"]\": true, \"[\\\"3\\\", \\\"4\\\"]\": false}}"));

        // map<set<text>, text> keys
        String innerKey1 = "[\"0\", \"1\"]";
        String fullKey1 = String.format("{\"%s\": \"%s\"}", Json.quoteAsJsonString(innerKey1), "a");
        String stringKey1 = Json.quoteAsJsonString(fullKey1);
        String innerKey2 = "[\"3\", \"4\"]";
        String fullKey2 = String.format("{\"%s\": \"%s\"}", Json.quoteAsJsonString(innerKey2), "b");
        String stringKey2 = Json.quoteAsJsonString(fullKey2);
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"nestedsetmap\": {\"" + stringKey1 + "\": true, \"" + stringKey2 + "\": false}}");
        assertRows(execute("SELECT JSON k, nestedsetmap FROM %s"), row("{\"k\": 0, \"nestedsetmap\": {\"" + stringKey1 + "\": true, \"" + stringKey2 + "\": false}}"));

        // set<int> keys in a frozen map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"frozensetmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, frozensetmap FROM %s"), row("{\"k\": 0, \"frozensetmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));
    }

    @Test
    public void testInsertJsonSyntaxWithTuplesAndUDTs() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b frozen<set<int>>, c tuple<int, int>)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "a frozen<" + typeName + ">, " +
                "b tuple<int, boolean>)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": [1, 2, 3], \"c\": [0, 1]}, \"b\": [0, true]}");
        assertRows(execute("SELECT k, a.a, a.b, a.c, b FROM %s"), row(0, 0, set(1, 2, 3), tuple(0, 1), tuple(0, true)));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": [1, 2, 3], \"c\": null}, \"b\": null}");
        assertRows(execute("SELECT k, a.a, a.b, a.c, b FROM %s"), row(0, 0, set(1, 2, 3), null, null));
    }

    // done for CASSANDRA-11146
    @Test
    public void testAlterUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "a frozen<" + typeName + ">)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0}}");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0}}"));

        schemaChange("ALTER TYPE " + KEYSPACE + "." + typeName + " ADD b boolean");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0, \"b\": null}}"));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": true}}");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0, \"b\": true}}"));
    }

    // done for CASSANDRA-11048
    @Test
    public void testJsonThreadSafety() throws Throwable
    {
        int numThreads = 10;
        final int numRows = 5000;

        createTable("CREATE TABLE %s (" +
                "k text PRIMARY KEY, " +
                "v text)");

        for (int i = 0; i < numRows; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", "" + i, "" + i);

        long seed = System.nanoTime();
        System.out.println("Seed " + seed);
        final Random rand = new Random(seed);

        final Runnable worker = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    for (int i = 0; i < numRows; i++)
                    {
                        String key = "" + rand.nextInt(numRows);
                        assertRows(execute("SELECT JSON * FROM %s WHERE k = ?", key),
                                row(String.format("{\"k\": \"%s\", \"v\": \"%s\"}", key, key)));
                    }
                }
                catch (Throwable exc)
                {
                    exc.printStackTrace();
                    fail(exc.getMessage());
                }
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++)
            futures.add(executor.submit(worker));

        for (Future future : futures)
            future.get(30, TimeUnit.SECONDS);

        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
    }

    @Test
    public void emptyStringJsonSerializationTest() throws Throwable
    {
        createTable("create table %s(id INT, name TEXT, PRIMARY KEY(id));");
        execute("insert into %s(id, name) VALUES (0, 'Foo');");
        execute("insert into %s(id, name) VALUES (2, '');");
        execute("insert into %s(id, name) VALUES (3, null);");

        assertRows(execute("SELECT JSON * FROM %s"),
                   row("{\"id\": 0, \"name\": \"Foo\"}"),
                   row("{\"id\": 2, \"name\": \"\"}"),
                   row("{\"id\": 3, \"name\": null}"));
    }

    // CASSANDRA-14286
    @Test
    public void testJsonOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s(a INT, b INT, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s(a, b) VALUES (20, 30);");
        execute("INSERT INTO %s(a, b) VALUES (100, 200);");

        assertRows(execute("SELECT JSON a, b FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"b\": 30}"),
                   row("{\"a\": 100, \"b\": 200}"));

        assertRows(execute("SELECT JSON a, b FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"b\": 200}"),
                   row("{\"a\": 20, \"b\": 30}"));

        assertRows(execute("SELECT JSON a FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100}"),
                   row("{\"a\": 20}"));

        // Check ordering with alias
        assertRows(execute("SELECT JSON a, b as c FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"c\": 30}"),
                   row("{\"a\": 100, \"c\": 200}"));

        assertRows(execute("SELECT JSON a, b as c FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"c\": 200}"),
                   row("{\"a\": 20, \"c\": 30}"));

        // Check ordering with CAST
        assertRows(execute("SELECT JSON a, CAST(b AS FLOAT) FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"cast(b as float)\": 30.0}"),
                   row("{\"a\": 100, \"cast(b as float)\": 200.0}"));

        assertRows(execute("SELECT JSON a, CAST(b AS FLOAT) FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"cast(b as float)\": 200.0}"),
                   row("{\"a\": 20, \"cast(b as float)\": 30.0}"));
    }

    @Test
     public void testInsertAndSelectJsonSyntaxWithEmptyAndNullValues() throws Throwable
     {
         createTable("create table %s(id INT, name TEXT, name_asc ASCII, bytes BLOB, PRIMARY KEY(id));");

         // Test with empty values

         execute("INSERT INTO %s JSON ?", "{\"id\": 0, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}");
         assertRows(execute("SELECT * FROM %s WHERE id=0"), row(0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "", ""));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 0"),
                    row("{\"id\": 0, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}"));

         execute("INSERT INTO %s(id, name, name_asc, bytes) VALUES (1, ?, ?, ?);", "", "", ByteBufferUtil.EMPTY_BYTE_BUFFER);
         assertRows(execute("SELECT * FROM %s WHERE id=1"), row(1, ByteBufferUtil.EMPTY_BYTE_BUFFER, "", ""));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 1"),
                    row("{\"id\": 1, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}"));

         // Test with null values

         execute("INSERT INTO %s JSON ?", "{\"id\": 2, \"bytes\": null, \"name\": null, \"name_asc\": null}");
         assertRows(execute("SELECT * FROM %s WHERE id=2"), row(2, null, null, null));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 2"),
                    row("{\"id\": 2, \"bytes\": null, \"name\": null, \"name_asc\": null}"));

         execute("INSERT INTO %s(id, name, name_asc, bytes) VALUES (3, ?, ?, ?);", null, null, null);
         assertRows(execute("SELECT * FROM %s WHERE id=3"), row(3, null, null, null));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 3"),
                 row("{\"id\": 3, \"bytes\": null, \"name\": null, \"name_asc\": null}"));
     }

    @Test
    public void testJsonWithNaNAndInfinity() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, f1 float, f2 float, f3 float, d1 double, d2 double, d3 double)");
        execute("INSERT INTO %s (pk, f1, f2, f3, d1, d2, d3) VALUES (?, ?, ?, ?, ?, ?, ?)",
                1, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        // JSON does not support NaN, Infinity and -Infinity values. Most of the parser convert them into null.
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"pk\": 1, \"d1\": null, \"d2\": null, \"d3\": null, \"f1\": null, \"f2\": null, \"f3\": null}"));
    }

    @Test
    public void testDurationJsonRoundtrip() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, d duration)");
        execute("INSERT INTO %s (pk, d) VALUES (1, 6h40m)");
        UntypedResultSet res = execute("SELECT JSON * FROM %s WHERE pk = 1");
        UntypedResultSet.Row r = res.one();
        String json = r.getString("[json]");
        execute("DELETE FROM %s WHERE pk = 1");
        execute("INSERT INTO %s JSON '"+json+"'");
        res = execute("SELECT JSON * FROM %s WHERE pk = 1");
        assertEquals(json, res.one().getString("[json]"));

    }
}
"""

# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# This file contains tests which check Scylla-specific features that do
# not exist on AWS. So all these tests are skipped when running with "--aws".

import pytest
import requests
import json
from util import random_string

# Test that the "/localnodes" request works, returning at least the one node.
# TODO: A more through test would need to start a cluster with multiple nodes
# in multiple data centers, and check that we can get a list of nodes in each
# data center. But this test framework cannot yet test that.
def test_localnodes(scylla_only, dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url + '/localnodes', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) >= 1

# The following test crashes Scylla when removing the table...
# Insert 10K of 10K strings (totalling about 100MB of data) into one
# partition:
def test_big(scylla_only, test_table_sn):
    p = random_string()
    str = 'x' * 10240
    with test_table_sn.batch_writer() as batch:
        for i in range(10000):
            batch.put_item({'p': p, 'c': i, 's': str})

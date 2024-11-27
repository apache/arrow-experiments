# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import urllib.request
import pyarrow as pa
import time

ARROW_STREAM_FORMAT = 'application/vnd.apache.arrow.stream'

start_time = time.time()

response = urllib.request.urlopen('http://localhost:8008')
content_type = response.headers['Content-Type']
if not content_type.startswith(ARROW_STREAM_FORMAT):
    raise ValueError(f"Expected {ARROW_STREAM_FORMAT}, got {content_type}")

batches = []

with pa.ipc.open_stream(response) as reader:
    schema = reader.schema
    try:
        while True:
            batches.append(reader.read_next_batch())
    except StopIteration:
        pass

# or:
# with pa.ipc.open_stream(response) as reader:
#     schema = reader.schema
#     batches = [b for b in reader]

end_time = time.time()
execution_time = end_time - start_time

print(f"{len(batches)} record batches received")
print(f"{execution_time} seconds elapsed")

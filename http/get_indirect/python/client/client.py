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

import requests
import json
import os
import pyarrow as pa


HOST = "http://localhost:8008/"

JSON_FORMAT = "application/json"
ARROW_STREAM_FORMAT = "application/vnd.apache.arrow.stream"

json_response = requests.get(HOST)

response_status = json_response.status_code
if not response_status == 200:
    raise ValueError(f"Expected response status 200, got {response_status}")

content_type = json_response.headers.get("Content-Type", "")
if not content_type.startswith(JSON_FORMAT):
    raise ValueError(f"Expected content type {JSON_FORMAT}, got {content_type}")

print("Downloaded JSON file listing.")

parsed_data = json_response.json()
uris = [file["uri"] for file in parsed_data["arrow_stream_files"]]

if not all(uri.endswith(".arrows") for uri in uris):
    raise ValueError(f"Some listed files do not have extension '.arrows'")

print(f"Parsed JSON and found {len(uris)} Arrow stream files.")

tables = {}

for uri in uris:
    arrow_response = requests.get(uri)

    response_status = arrow_response.status_code
    if not response_status == 200:
        raise ValueError(f"Expected response status 200, got {response_status}")

    content_type = arrow_response.headers.get("Content-Type", "")
    if not content_type.startswith(ARROW_STREAM_FORMAT):
        raise ValueError(f"Expected content type {ARROW_STREAM_FORMAT}, got {content_type}")
    
    filename = os.path.basename(uri)

    print(f"Downloaded file '{filename}'.")

    tablename = os.path.splitext(filename)[0]
    with pa.ipc.open_stream(arrow_response.content) as reader:
        tables[tablename] = reader.read_all()

    print(f"Loaded into in-memory Arrow table '{tablename}'.")

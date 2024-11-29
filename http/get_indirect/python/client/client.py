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

if json_response.status_code == 200 and JSON_FORMAT in json_response.headers.get('Content-Type', ''):
    parsed_data = json_response.json()
    filenames = [file['filename'] for file in parsed_data['arrow_stream_files']]
    uris = [HOST + filename for filename in filenames]
    print("Downloaded and parsed JSON file listing. Found " + str(len(filenames)) + " files.")
else:
    raise ValueError("Unexpected Content-Type")

tables = {}

for uri in uris:
    arrow_response = requests.get(uri)
    if arrow_response.status_code == 200 and ARROW_STREAM_FORMAT in arrow_response.headers.get('Content-Type', ''):
        filename = os.path.basename(uri)
        tablename = os.path.splitext(filename)[0]
        with pa.ipc.open_stream(arrow_response.content) as reader:
            tables[tablename] = reader.read_all()
        print("Downloaded file " + filename + ".")
    else:
        raise ValueError("Unexpected Content-Type")

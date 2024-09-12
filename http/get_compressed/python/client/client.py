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

URI = "http://localhost:8008"
ARROW_STREAM_FORMAT = "application/vnd.apache.arrow.stream"


def make_request(uri, coding):
    # urllib.request.urlopen() always sends an HTTP/1.1 request
    # with Accept-Encoding: identity, so we need to setup a request
    # object to customize the request headers
    request = urllib.request.Request(
        uri,
        headers={
            "Accept-Encoding": f"{coding}, *;q=0",
        },
    )
    response = urllib.request.urlopen(request)
    content_type = response.headers["Content-Type"]
    if content_type != ARROW_STREAM_FORMAT:
        raise ValueError(f"Expected {ARROW_STREAM_FORMAT}, got {content_type}")
    if coding == "identity":
        return response
    # IANA nomenclature for Brotli is "br" and not "brotli"
    compression = "brotli" if coding == "br" else coding
    return pa.CompressedInputStream(response, compression)


def request_and_process(uri, coding):
    batches = []
    log_prefix = f"{'[' + coding + ']':>10}:"
    print(f"{log_prefix} Requesting data from {uri} with `{coding}` encoding.")
    start_time = time.time()
    response = make_request(uri, coding)
    with pa.ipc.open_stream(response) as reader:
        schema = reader.schema
        time_to_schema = time.time() - start_time
        try:
            batch = reader.read_next_batch()
            time_to_first_batch = time.time() - start_time
            batches.append(batch)
            while True:
                batch = reader.read_next_batch()
                batches.append(batch)
        except StopIteration:
            pass
        processing_time = time.time() - start_time
        reader_stats = reader.stats
    print(
        f"{log_prefix} Schema received in {time_to_schema:.3f} seconds."
        f" schema=({', '.join(schema.names)})."
    )
    print(
        f"{log_prefix} First batch received and processed in"
        f" {time_to_first_batch:.3f} seconds"
    )
    print(
        f"{log_prefix} Processing of all batches completed in"
        f" {processing_time:.3f} seconds."
    )
    print(f"{log_prefix}", reader_stats)
    return batches


request_and_process(URI, "identity")
request_and_process(URI, "zstd")
request_and_process(URI, "br")
request_and_process(URI, "gzip")

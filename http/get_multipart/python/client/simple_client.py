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

import email
import json
import pyarrow as pa
import sys
import time
import urllib.request

JSON_FORMAT = "application/json"
TEXT_FORMAT = "text/plain"
ARROW_STREAM_FORMAT = "application/vnd.apache.arrow.stream"

start_time = time.time()
response_parsing_time = 0  # time to parse the multipart message
arrow_stream_parsing_time = 0  # time to parse the Arrow stream


def parse_multipart_message(response, boundary, buffer_size=8192):
    """
    Parse a multipart/mixed HTTP response into a list of Message objects.

    Returns
    -------
    list of email.message.Message containing the parts of the multipart message.
    """
    global response_parsing_time
    buffer_size = max(buffer_size, 8192)
    buffer = bytearray(buffer_size)

    header = f'MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="{boundary}"\r\n\r\n'
    feedparser = email.parser.BytesFeedParser()
    feedparser.feed(header.encode("utf-8"))
    while bytes_read := response.readinto(buffer):
        start_time = time.time()
        feedparser.feed(buffer[0:bytes_read])
        response_parsing_time += time.time() - start_time
    start_time = time.time()
    message = feedparser.close()
    response_parsing_time += time.time() - start_time
    assert message.is_multipart()
    return message.get_payload()


def process_json_part(message):
    assert message.get_content_type() == JSON_FORMAT
    payload = part.get_payload()
    print(f"-- {len(payload)} bytes of JSON data:")
    try:
        PREVIW_SIZE = 5
        data = json.loads(payload)
        print("[")
        for i in range(min(PREVIW_SIZE, len(data))):
            print(f"  {data[i]}")
        if len(data) > PREVIW_SIZE:
            print(f"  ...+{len(data) - PREVIW_SIZE} entries...")
        print("]")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON data: {e}\n", file=sys.stderr)
    return data


def process_arrow_stream_message(message):
    global arrow_stream_parsing_time
    assert message.get_content_type() == ARROW_STREAM_FORMAT
    payload = part.get_payload(decode=True)
    print(f"-- {len(payload)} bytes of Arrow data:")
    num_batches = 0
    num_records = 0
    start_time = time.time()
    with pa.ipc.open_stream(payload) as reader:
        schema = reader.schema
        print(f"Schema: \n{schema}\n")
        try:
            while True:
                batch = reader.read_next_batch()
                num_batches += 1
                num_records += batch.num_rows
        except StopIteration:
            pass
    arrow_stream_parsing_time = time.time() - start_time
    print(f"Parsed {num_records} records in {num_batches} batch(es)")


def process_text_part(message):
    assert message.get_content_type() == TEXT_FORMAT
    payload = part.get_payload()
    print("-- Text Message:")
    print(payload, end="")
    print("-- End of Text Message --")


response = urllib.request.urlopen("http://localhost:8008?include_footnotes")

content_type = response.headers.get_content_type()
if content_type != "multipart/mixed":
    raise ValueError(f"Expected multipart/mixed Content-Type, got {content_type}")
boundary = response.headers.get_boundary()
if boundary is None or len(boundary) == 0:
    raise ValueError("No multipart boundary found in Content-Type header")

parts = parse_multipart_message(response, boundary, buffer_size=64 * 1024)
batches = None
for part in parts:
    content_type = part.get_content_type()
    if content_type == JSON_FORMAT:
        process_json_part(part)
    elif content_type == ARROW_STREAM_FORMAT:
        batches = process_arrow_stream_message(part)
    elif content_type == TEXT_FORMAT:
        process_text_part(part)

end_time = time.time()
execution_time = end_time - start_time

rel_response_parsing_time = response_parsing_time / execution_time
rel_arrow_stream_parsing_time = arrow_stream_parsing_time / execution_time
print(f"{execution_time:.3f} seconds elapsed")
print(
    f"""{response_parsing_time:.3f} seconds \
({rel_response_parsing_time * 100:.2f}%) \
seconds parsing multipart/mixed response"""
)
print(
    f"""{arrow_stream_parsing_time:.3f} seconds \
({rel_arrow_stream_parsing_time * 100:.2f}%) \
seconds parsing Arrow stream"""
)

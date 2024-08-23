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

from random import choice, randint
from http.server import BaseHTTPRequestHandler, HTTPServer
import io
import json
import secrets
import string
import time

import pyarrow as pa

# configuration: use chunked transfer encoding for HTTP/1.1 responses?
CHUNKED_ENCODING = True


def random_string(alphabet, length):
    return "".join(choice(alphabet) for _ in range(length))


def random_name(initial):
    length = randint(3, 7)
    return initial + random_string(string.ascii_lowercase, length)


def example_tickers(num_tickers):
    tickers = []
    while len(tickers) < num_tickers:
        length = randint(3, 4)
        random_ticker = random_string(string.ascii_uppercase, length)
        if random_ticker not in tickers:
            tickers.append(random_ticker)
    return tickers


def example_json_data(tickers):
    json_data = []
    for ticker in tickers:
        description = ""
        for c in ticker:
            description = " ".join(random_name(c) for c in ticker)
        json_data.append(
            {
                "ticker": ticker,
                "description": description,
            }
        )
    return json_data


the_schema = pa.schema(
    [
        ("ticker", pa.utf8()),
        ("price", pa.int64()),
        ("volume", pa.int64()),
    ]
)


def example_batch(tickers, length):
    data = {"ticker": [], "price": [], "volume": []}
    for _ in range(length):
        data["ticker"].append(choice(tickers))
        data["price"].append(randint(1, 1000) * 100)
        data["volume"].append(randint(1, 10000))

    return pa.RecordBatch.from_pydict(data, the_schema)


def example_batches(tickers):
    # these parameters are chosen to generate a response
    # of ~1 GB and chunks of ~140 KB.
    total_records = 42_000_000
    batch_len = 6 * 1024
    # all the batches sent are random slices of the larger base batch
    base_batch = example_batch(tickers, length=8 * batch_len)
    batches = []
    records = 0
    while records < total_records:
        length = min(batch_len, total_records - records)
        offset = randint(0, base_batch.num_rows - length - 1)
        batch = base_batch.slice(offset, length)
        batches.append(batch)
        records += length
    return batches


# end of example data generation


def random_multipart_boundary():
    """
    Generate a random boundary string for a multipart response.

    Uses a cryptographically secure random number generator to generate a
    random boundary string for a multipart response. The boundary string has
    enough entropy to make it impossible that it will be repeated in the
    response body.

    Use a new boundary string for each multipart response so that once the
    secret is revealed to the client, it won't be possible to exploit it to
    create a malicious response.
    """
    # 28 bytes (224 bits) of entropy is enough to make a collision impossible.
    # See [1] for a mathematical discussion.
    #
    # The 28 bytes are encoded into URL-safe characters so the string ends
    # up longer than 28 characters. RFC1341 [2] recommends a maximum boundary
    # length of 70 characters, so we're well within that limit.
    #
    # [1] https://preshing.com/20110504/hash-collision-probabilities/
    # [2] https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html
    return secrets.token_urlsafe(28)


def gen_arrow_multipart_buffers(boundary, schema, source, is_last_part=False):
    """
    Generate buffers for the Arrow Stream part of a multipart response.

    That is, an HTTP response started with the header:

        Content-type: multipart/mixed; boundary=the_boundary_string

    The buffers, when taken together, will form the following structure:

        --the_boundary_string<CR><LF>
        Content-Type: application/vnd.apache.arrow.stream<CR><LF>
        <CR><LF>
        <Arrow Stream data>
        <CR><LF>

    If is_last_part is True, the boundary string will be appended with two
    hyphens at the end of the last buffer to indicate the end of the multipart
    response:

        --the_boundary_string--<CR><LF>
    """
    with io.BytesIO() as sink, pa.ipc.new_stream(sink, schema) as writer:
        sink.write(
            f"--{boundary}\r\n"
            "Content-Type: application/vnd.apache.arrow.stream\r\n"
            "\r\n".encode("utf-8")
        )
        for batch in source:
            writer.write_batch(batch)
            sink.truncate()
            with sink.getbuffer() as buffer:
                yield buffer
            sink.seek(0)

        writer.close()
        sink.write("\r\n".encode("utf-8"))
        if is_last_part:
            sink.write(f"--{boundary}--\r\n".encode("utf-8"))
        sink.truncate()
        with sink.getbuffer() as buffer:
            yield buffer


def gen_json_multipart_buffers(boundary, json_data, is_last_part=False):
    """
    Generate buffers for the JSON part of a multipart response.

    That is, an HTTP response started with the header:

        Content-type: multipart/mixed; boundary=the_boundary_string

    The buffer will have the following structure:

        --the_boundary_string<CR><LF>
        Content-Type: application/json<CR><LF>
        <CR><LF>
        <serialized JSON data>
        <CR><LF>

    If is_last_part is True, the boundary string will be appended with two
    hyphens at the end of the buffer to indicate the end of the multipart
    response:

        --the_boundary_string--<CR><LF>

    Allocation of a big string for the JSON data is avoided by appending the
    JSON data directly to the same output buffer.
    """
    with io.BytesIO() as sink:
        with io.TextIOWrapper(sink, encoding="utf-8", write_through=True) as wrapper:
            wrapper.write(f"--{boundary}\r\n" "Content-Type: application/json\r\n\r\n")
            json.dump(json_data, wrapper)
            wrapper.write("\r\n")
            if is_last_part:
                wrapper.write(f"--{boundary}--\r\n")
            with sink.getbuffer() as buffer:
                yield buffer


def multipart_buffer_from_string(boundary, content_type, text, is_last_part=False):
    close_delimiter = f"--{boundary}--\r\n" if is_last_part else ""
    return (
        f"--{boundary}\r\n"
        f"Content-Type: {content_type}\r\n\r\n"
        f"{text}\r\n{close_delimiter}".encode("utf-8")
    )


class MyRequestHandler(BaseHTTPRequestHandler):
    """
    Multipart response handler for a simple HTTP server.

    This HTTP request handler serves a multipart/mixed response containing
    a JSON data part, followed by an Arrow Stream part and an optional text
    footer as the last part.

    The Arrow data is randomly generated "trading data" with a schema consisting
    of a ticker, price (in cents), and volume. The JSON header contains all the
    tickers and their descriptions. This could be returned as an Arrow table as
    well, but to illustrate the use of multiple parts in a response, it is sent
    as JSON.

    To make things more... mixed, a third part is added to the response: a
    plaintext footer containing footnotes about the request. This part is
    optional and only included if the client requests it by sending a query
    parameter `include_footnotes`.
    """

    _include_footnotes = False
    _start_arrow_stream_time = None
    _end_arrow_stream_time = None
    _number_of_arrow_data_chunks = 0
    _bytes_sent_on_arrow_stream = 0

    def _resolve_json_data_header(self):
        return the_json_data

    def _resolve_batches(self):
        return pa.RecordBatchReader.from_batches(the_schema, all_batches)

    def _build_footnotes(self):
        num_batches = len(all_batches)
        elapsed_time = self._end_arrow_stream_time - self._start_arrow_stream_time
        num_chunks = self._number_of_arrow_data_chunks
        avg_chunk_size = self._bytes_sent_on_arrow_stream / num_chunks
        text = (
            f"Hello Client,\n\n{num_batches} Arrow batch(es) were sent in "
            f"{elapsed_time:.3f} seconds through {num_chunks} HTTP\nresponse chunks. "
            f"Average size of each chunk was {avg_chunk_size:.2f} bytes.\n"
            "\n--\nSincerely,\nThe Server\n"
        )
        return text

    def _gen_buffers(self, boundary, json_header, schema, source):
        # JSON header
        yield from gen_json_multipart_buffers(boundary, json_header)
        # Arrow data
        is_last_part = not self._include_footnotes
        self._start_arrow_stream_time = time.time()
        for buffer in gen_arrow_multipart_buffers(
            boundary, schema, source, is_last_part=is_last_part
        ):
            self._number_of_arrow_data_chunks += 1
            self._bytes_sent_on_arrow_stream += len(buffer)
            yield buffer
        self._end_arrow_stream_time = time.time()
        # Footnotes (optional)
        if self._include_footnotes:
            footnotes = self._build_footnotes()
            yield multipart_buffer_from_string(
                boundary, "text/plain", footnotes, is_last_part=True
            )

    def do_GET(self):
        ### note: always use urlparse in your applications.
        self._include_footnotes = self.path.endswith("?include_footnotes")
        ### in a real application the data would be resolved from a database or
        ### another source like a file and error handling would be done here
        ### before the 200 OK response starts being sent to the client.
        json_data_header = self._resolve_json_data_header()
        source = self._resolve_batches()

        if self.request_version == "HTTP/1.0":
            self.protocol_version = "HTTP/1.0"
            chunked = False
        else:
            self.protocol_version = "HTTP/1.1"
            chunked = CHUNKED_ENCODING

        self.send_response(200)
        boundary = random_multipart_boundary()
        self.send_header("Content-Type", f"multipart/mixed; boundary={boundary}")
        ### set these headers if testing with a local browser-based client:
        # self.send_header('Access-Control-Allow-Origin', 'http://localhost:8008')
        # self.send_header('Access-Control-Allow-Methods', 'GET')
        # self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")

        self.end_headers()

        for buffer in self._gen_buffers(boundary, json_data_header, the_schema, source):
            if chunked:
                self.wfile.write(f"{len(buffer):X}\r\n".encode("utf-8"))
            self.wfile.write(buffer)
            if chunked:
                self.wfile.write("\r\n".encode("utf-8"))
            self.wfile.flush()

        if chunked:
            self.wfile.write("0\r\n\r\n".encode("utf-8"))
            self.wfile.flush()


print("Generating example data...")
all_tickers = example_tickers(60)
all_batches = example_batches(all_tickers)
the_json_data = example_json_data(all_tickers)

server_address = ("localhost", 8008)
try:
    httpd = HTTPServer(server_address, MyRequestHandler)
    print(f"Serving on {server_address[0]}:{server_address[1]}...")
    httpd.serve_forever()
except KeyboardInterrupt:
    print("Shutting down server")
    httpd.socket.close()

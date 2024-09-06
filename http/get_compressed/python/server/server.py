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
import pyarrow as pa
import re
import socketserver
import string

# HTTP/1.1: Use chunked responses for HTTP/1.1 requests.
USE_CHUNKED_HTTP1_1_ENCODING = True
# HTTP/1.0: put entire response in a buffer, set the Content-Length header,
# and send it all at once. If False, stream the response into the socket.
BUFFER_HTTP1_0_CONTENT = False


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
    # of ~1 GB and chunks of ~140 KB (uncompressed)
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

# [L]inear [W]hite [S]pace pattern (HTTP/1.1 - RFC 2616)
LWS_RE = "(?:[ \\t]|\\r\\n[ \\t]+)*"
CONTENT_CODING_PATTERN = re.compile(
    r"%s(?:([A-Za-z]+|\*)%s(?:;%sq%s=(%s[01]+(?:\.\d{1,3})?))?%s)?"
    % (LWS_RE, LWS_RE, LWS_RE, LWS_RE, LWS_RE, LWS_RE)
)


def parse_accept_encoding(s):
    """
    Parse the Accept-Encoding request header value.

    Returns
    -------
    list of (str, float|None)
        The list of lowercase codings (or "*") and their qvalues in the order
        they appear in the header. The qvalue is None if not specified.
    """
    pieces = s.split(",")
    accepted = []
    for piece in pieces:
        m = re.fullmatch(CONTENT_CODING_PATTERN, piece)
        if m is None:
            raise ValueError(f"Malformed Accept-Encoding header: {s!r}")
        if m.group(1) is None:  # whitespace is skipped
            continue
        coding = m.group(1).lower()
        qvalue = m.group(2)
        pair = coding, float(qvalue) if qvalue is not None else None
        accepted.append(pair)
    return accepted


def check_parser(s, expected):
    try:
        parsed = parse_accept_encoding(s)
        # print("parsed:", parsed, "\nexpected:", expected)
        assert parsed == expected
    except ValueError as e:
        print(e)


check_parser("", [])
expected = [("gzip", None), ("zstd", 1.0), ("*", None)]
check_parser("gzip, zstd;q=1.0,  *", expected)
check_parser("gzip ,     zstd; q= 1.0  ,  *", expected)
expected = [("gzip", None), ("zstd", 1.0), ("*", 0.0)]
check_parser("gzip ,     zstd; q= 1.0 \t  \r\n ,*;q =0", expected)
expected = [("zstd", 1.0), ("gzip", 0.5), ("br", 0.8), ("identity", 0.0)]
check_parser("zstd;q=1.0, gzip;q=0.5, br;q=0.8, identity;q=0", expected)


def pick_coding(accept_encoding_header, available):
    """
    Pick the content-coding that the server should use to compress the response.

    Parameters
    ----------
    accept_encoding_header : str
        The value of the Accept-Encoding header from an HTTP request.
    available : list of str
        The content-codings that the server can provide in the order preferred
        by the server.

    Returns
    -------
    str
        The content-coding that the server should use to compress the response.
        "identity" is returned if no acceptable content-coding is found in the
        list of available codings.

        None if the client does not accept any of the available content-codings
        and doesn't accept "identity" (uncompressed) either. In this case,
        a "406 Not Acceptable" response should be sent.
    """
    accepted = parse_accept_encoding(accept_encoding_header)

    def value_or(value, default):
        return default if value is None else value

    if "identity" not in available:
        available = available + ["identity"]
    state = {}
    for coding, qvalue in accepted:
        qvalue = value_or(qvalue, 0.0001 if coding == "identity" else 1.0)
        if coding == "*":
            for coding in available:
                if coding not in state:
                    state[coding] = qvalue
        elif coding in available:
            state[coding] = qvalue
    # "identity" is always acceptable unless explicitly refused (;q=0)
    if "identity" not in state:
        state["identity"] = 0.0001
    # all the candidate codings are now in the state dictionary and we
    # have to consider only the ones that have the maximum qvalue
    max_qvalue = max(state.values())
    if max_qvalue == 0.0:
        return None
    for coding in available:
        if coding in state and state[coding] == max_qvalue:
            return coding
    return None


def check_picker(header, expected):
    available = ["zstd", "gzip"]  # no "br" an no "deflate"
    chosen = pick_coding(header, available)
    # print("Accept-Encoding:", header, "\nexpected:", expected, "\t\tchosen:", chosen)
    assert chosen == expected


check_picker("gzip, zstd;q=1.0,  *", "zstd")
check_picker("gzip ,     zstd; q= 1.0  ,  *", "zstd")
check_picker("gzip ,     zstd; q= 1.0 \t  \r\n ,*;q =0", "zstd")
check_picker("zstd;q=1.0, gzip;q=0.5, br;q=0.8, identity;q=0", "zstd")

check_picker("compress, gzip", "gzip")
check_picker("", "identity")
check_picker("*", "zstd")
check_picker("compress;q=0.5, gzip;q=1.0", "gzip")
check_picker("gzip;q=1.0, identity; q=0.5, *;q=0", "gzip")
check_picker("br, *;q=0", None)
check_picker("br", "identity")


class LateClosingBytesIO(io.BytesIO):
    """
    BytesIO that does not close on close().

    When a stream wrapping a a file-like object is closed, the underlying
    file-like object is also closed. This subclass prevents that from
    happening by overriding the close method.

    If we close a RecordBatchStreamWriter wrapping a BytesIO object, we want
    to be able to create a memory view of the buffer. But that is only possible
    if the BytesIO object is not closed yet.
    """
    def close(self):
        pass

    def close_now(self):
        super().close()


class SocketWriterSink(socketserver._SocketWriter):
    """Wrapper to make wfile usable as a sink for Arrow stream writing."""
    def __init__(self, wfile):
        self.wfile = wfile

    def writable(self):
        return True

    def write(self, b):
        self.wfile.write(b)

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        """Do nothing so Arrow stream wrappers don't close the socket."""
        pass


def stream_all(schema, source, coding, sink):
    if coding == "identity":
        # source: RecordBatchReader
        #   |> writer: RecordBatchStreamWriter
        #   |> sink: file-like
        with pa.ipc.new_stream(sink, schema) as writer:
            for batch in source:
                writer.write_batch(batch)
            writer.close()  # write EOS marker and flush
    else:
        # IANA nomenclature for Brotli is "br" and not "brotli"
        compression = "brotli" if coding == "br" else coding
        with pa.CompressedOutputStream(sink, compression) as compressor:
            # source: RecordBatchReader
            #   |> writer: RecordBatchStreamWriter
            #   |> compressor: CompressedOutputStream
            #   |> sink: file-like
            with pa.ipc.new_stream(compressor, schema) as writer:
                for batch in source:
                    writer.write_batch(batch)
                writer.close()  # write EOS marker and flush
            # ensure buffered data is compressed and written to the sink
            compressor.close()


def generate_single_buffer(schema, source, coding):
    """
    Put all the record batches from the source into a single buffer.

    If `coding` is "identity", the source is written to the buffer as is.
    Otherwise, the source is compressed using the given coding.
    """
    # the sink holds the buffer and we give a view of it to the caller
    with LateClosingBytesIO() as sink:
        stream_all(schema, source, coding, sink)
        # zero-copy buffer access using getbuffer() keeping the sink alive
        # after the yield statement until this function is done executing
        with sink.getbuffer() as buffer:
            yield buffer
        sink.close_now()


def generate_buffers(schema, source, coding):
    # the sink holds the buffer and we give a view of it to the caller
    with LateClosingBytesIO() as sink:
        # keep buffering until we have at least MIN_BUFFER_SIZE bytes
        # in the buffer before yielding it to the caller
        MIN_BUFFER_SIZE = 256 * 1024
        if coding == "identity":
            # source: RecordBatchReader
            #   |> writer: RecordBatchStreamWriter
            #   |> sink: LateClosingBytesIO
            writer = pa.ipc.new_stream(sink, schema)
            try:
                while True:
                    writer.write_batch(source.read_next_batch())
                    if sink.tell() >= MIN_BUFFER_SIZE:
                        sink.truncate()
                        with sink.getbuffer() as buffer:
                            yield buffer
                        sink.seek(0)
            except StopIteration:
                pass

            writer.close()  # write EOS marker and flush
        else:
            compression = "brotli" if coding == "br" else coding
            with pa.CompressedOutputStream(sink, compression) as compressor:
                # source: RecordBatchReader
                #   |> writer: RecordBatchStreamWriter
                #   |> compressor: CompressedOutputStream
                #   |> sink: LateClosingBytesIO
                writer = pa.ipc.new_stream(compressor, schema)
                try:
                    while True:
                        writer.write_batch(source.read_next_batch())
                        if sink.tell() >= MIN_BUFFER_SIZE:
                            sink.truncate()
                            with sink.getbuffer() as buffer:
                                yield buffer
                            sink.seek(0)
                except StopIteration:
                    pass

                writer.close()  # write EOS marker and flush
                compressor.close()

            sink.truncate()
            with sink.getbuffer() as buffer:
                yield buffer
            sink.close_now()


AVAILABLE_ENCODINGS = ["zstd", "br", "gzip"]
"""
List of available content-codings as used in HTTP.

Note that Arrow stream classes refer to Brotli as "brotli" and not "br".
"""


class MyRequestHandler(BaseHTTPRequestHandler):
    """
    Response handler for a simple HTTP server.

    This HTTP request handler serves a compressed HTTP response with an Arrow
    stream in it or a (TODO) compressed Arrow stream in a uncompressed HTTP
    response.

    The Arrow data is randomly generated "trading data" with a schema consisting
    of a ticker, price (in cents), and volume.
    """

    def _resolve_batches(self):
        return pa.RecordBatchReader.from_batches(the_schema, all_batches)

    def _send_not_acceptable(self, accept_encoding, parsing_error=None):
        self.send_response(406, "Not Acceptable")
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        if parsing_error:
            message = f"Error parsing `Accept-Encoding` header: {parsing_error}\n"
        else:
            message = "None of the available codings are accepted by this client.\n"
        message += f"`Accept-Encoding` header was {accept_encoding!r}.\n"
        self.wfile.write(bytes(message, "utf-8"))

    def do_GET(self):
        # HTTP/1.0 requests don't get chunked responses
        if self.request_version == "HTTP/1.0":
            self.protocol_version = "HTTP/1.0"
            chunked = False
        else:
            self.protocol_version = "HTTP/1.1"
            chunked = USE_CHUNKED_HTTP1_1_ENCODING

        coding = None
        parsing_error = None
        accept_encoding = self.headers.get("Accept-Encoding")
        if accept_encoding is None:
            # if the Accept-Encoding header is not explicitly set, return the
            # uncompressed data for HTTP/1.0 requests and compressed data for
            # HTTP/1.1 requests with the safest compression format choice: "gzip".
            coding = "identity" if self.request_version == "HTTP/1.0" else "gzip"
        else:
            try:
                coding = pick_coding(accept_encoding, AVAILABLE_ENCODINGS)
            except ValueError as e:
                parsing_error = e

        if coding is None:
            self._send_not_acceptable(accept_encoding, parsing_error)
            return

        ### in a real application the data would be resolved from a database or
        ### another source like a file and error handling would be done here
        ### before the 200 OK response starts being sent to the client.
        source = self._resolve_batches()

        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        if coding != "identity":
            self.send_header("Content-Encoding", coding)
        ### set these headers if testing with a local browser-based client:
        # self.send_header('Access-Control-Allow-Origin', 'http://localhost:8008')
        # self.send_header('Access-Control-Allow-Methods', 'GET')
        # self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for buffer in generate_buffers(the_schema, source, coding):
                self.wfile.write(f"{len(buffer):X}\r\n".encode("utf-8"))
                self.wfile.write(buffer)
                self.wfile.write("\r\n".encode("utf-8"))
            self.wfile.write("0\r\n\r\n".encode("utf-8"))
        else:
            if BUFFER_HTTP1_0_CONTENT:
                for buffer in generate_single_buffer(the_schema, source, coding):
                    self.send_header("Content-Length", str(len(buffer)))
                    self.end_headers()
                    self.wfile.write(buffer)
                    break
            else:
                self.end_headers()
                sink = SocketWriterSink(self.wfile)
                stream_all(the_schema, source, coding, sink)


print("Generating example data...")

all_tickers = example_tickers(60)
all_batches = example_batches(all_tickers)

server_address = ("localhost", 8008)
try:
    httpd = HTTPServer(server_address, MyRequestHandler)
    print(f"Serving on {server_address[0]}:{server_address[1]}...")
    httpd.serve_forever()
except KeyboardInterrupt:
    print("Shutting down server")
    httpd.socket.close()

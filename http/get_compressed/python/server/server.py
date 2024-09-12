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
import pyarrow.compute as pc
import re
import socketserver
import string

# use dictionary encoding for the ticker column
USE_DICTIONARY_ENCODING = True


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


the_ticker_type = (
    pa.dictionary(pa.int32(), pa.utf8()) if USE_DICTIONARY_ENCODING else pa.utf8()
)
the_schema = pa.schema(
    [
        ("ticker", the_ticker_type),
        ("price", pa.int64()),
        ("volume", pa.int64()),
    ]
)


def example_batch(tickers, length):
    ticker_indices = []
    price = []
    volume = []
    for _ in range(length):
        ticker_indices.append(randint(0, len(tickers) - 1))
        price.append(randint(1, 1000) * 100)
        volume.append(randint(1, 10000))
    ticker = (
        pa.DictionaryArray.from_arrays(ticker_indices, tickers)
        if USE_DICTIONARY_ENCODING
        else pc.take(tickers, ticker_indices, boundscheck=False)
    )
    return pa.RecordBatch.from_arrays([ticker, price, volume], schema=the_schema)


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
# tokenizer pattern to support the Accept-Encoding header parser
TOKENS_PATTERN = re.compile(
    r"(?P<ID>[A-Za-z][A-Za-z0-9]*|\*)"  # a name or a wildcard token
    r"|(?P<COMMA>,)"
    r"|(?P<SEMI>;)"
    r"|(?P<EQ>=)"
    r"|(?P<NUM>\d+(\.\d{1,3})?)"
    f"|(?P<SKIP>{LWS_RE})"
    r"|(?P<MISMATCH>.+)"
)


def unexpected(header_name, label, value):
    msg = f"Malformed {header_name} header: unexpected {label} at {value!r}"
    return ValueError(msg)


def tokenize(header_name, s):
    for mo in re.finditer(TOKENS_PATTERN, s):
        kind = mo.lastgroup
        if kind == "SKIP":
            continue
        elif kind == "MISMATCH":
            raise unexpected(header_name, "character", mo.group())
        yield [kind, mo.group()]


def parse_accept_encoding(s):
    """
    Parse the Accept-Encoding request header value.

    Returns
    -------
    list of (str, float|None)
        The list of lowercase codings (or "*") and their qvalues in the order
        they appear in the header. The qvalue is None if not specified.
    """
    AE = "Accept-Encoding"
    tokens = tokenize(AE, s)

    def expect(expected_kind):
        kind, value = next(tokens)
        if kind != expected_kind:
            raise unexpected(AE, "token", value)
        return value

    accepted = []
    while True:
        try:
            coding = None
            qvalue = None
            coding = expect("ID").lower()
            kind, value = next(tokens)
            if kind == "COMMA":
                accepted.append((coding, qvalue))
                continue
            if kind == "SEMI":
                value = expect("ID")
                if value != "q":
                    raise unexpected(AE, "token", value)
                expect("EQ")
                qvalue = float(expect("NUM"))
                expect("COMMA")
                accepted.append((coding, qvalue))
                continue
            raise unexpected(AE, "token", value)
        except StopIteration:
            break
    # this parser ignores any unfinished ;q=NUM sequence or trailing commas
    if coding is not None:
        accepted.append((coding, qvalue))
    return accepted


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


def generate_chunk_buffers(schema, source, coding):
    # the sink holds the buffer and we give a view of it to the caller
    with LateClosingBytesIO() as sink:
        # keep buffering until we have at least MIN_BUFFER_SIZE bytes
        # in the buffer before yielding it to the caller. Setting it
        # to 1 means we yield as soon as the compression blocks are
        # formed and reach the sink buffer.
        MIN_BUFFER_SIZE = 64 * 1024
        if coding == "identity":
            # source: RecordBatchReader
            #   |> writer: RecordBatchStreamWriter
            #   |> sink: LateClosingBytesIO
            writer = pa.ipc.new_stream(sink, schema)
            for batch in source:
                writer.write_batch(batch)
                if sink.tell() >= MIN_BUFFER_SIZE:
                    sink.truncate()
                    with sink.getbuffer() as buffer:
                        yield buffer
                    sink.seek(0)

            writer.close()  # write EOS marker and flush
        else:
            compression = "brotli" if coding == "br" else coding
            with pa.CompressedOutputStream(sink, compression) as compressor:
                # has the first buffer been yielded already?
                sent_first = False
                # source: RecordBatchReader
                #   |> writer: RecordBatchStreamWriter
                #   |> compressor: CompressedOutputStream
                #   |> sink: LateClosingBytesIO
                writer = pa.ipc.new_stream(compressor, schema)
                for batch in source:
                    writer.write_batch(batch)
                    # we try to yield a buffer ASAP no matter how small
                    if not sent_first and sink.tell() == 0:
                        compressor.flush()
                    pos = sink.tell()
                    if pos >= MIN_BUFFER_SIZE or (not sent_first and pos >= 1):
                        sink.truncate()
                        with sink.getbuffer() as buffer:
                            yield buffer
                        sink.seek(0)
                        sent_first = True

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
            chunked = True

        coding = None
        parsing_error = None
        accept_encoding = self.headers.get("Accept-Encoding")
        if accept_encoding is None:
            # if the Accept-Encoding header is not explicitly set, return the
            # uncompressed data for HTTP/1.0 requests and compressed data for
            # HTTP/1.1 requests with the safest compression format choice: "gzip".
            coding = (
                "identity"
                if self.request_version == "HTTP/1.0"
                or ("gzip" not in AVAILABLE_ENCODINGS)
                else "gzip"
            )
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
        ### set these headers if testing with a local browser-based client:
        # self.send_header('Access-Control-Allow-Origin', 'http://localhost:8008')
        # self.send_header('Access-Control-Allow-Methods', 'GET')
        # self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        # suggest a default filename in case this response is saved by the user
        self.send_header("Content-Disposition", r'attachment; filename="output.arrows"')

        if coding != "identity":
            self.send_header("Content-Encoding", coding)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for buffer in generate_chunk_buffers(the_schema, source, coding):
                self.wfile.write(f"{len(buffer):X}\r\n".encode("utf-8"))
                self.wfile.write(buffer)
                self.wfile.write("\r\n".encode("utf-8"))
            self.wfile.write("0\r\n\r\n".encode("utf-8"))
        else:
            self.end_headers()
            sink = SocketWriterSink(self.wfile)
            for buffer in generate_chunk_buffers(the_schema, source, coding):
                sink.write(buffer)


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

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

import pyarrow as pa
import urllib.parse
from random import randbytes
from http.server import BaseHTTPRequestHandler, HTTPServer
import io


def get_post_data_and_schema(total_records: int):
    schema = pa.schema([('a', pa.int64()), ('b', pa.int64()),
                        ('c', pa.int64()), ('d', pa.int64())])
    length = 4096
    ncolumns = 4

    arrays = []

    for x in range(0, ncolumns):
        buffer = pa.py_buffer(randbytes(length * 8))
        arrays.append(
            pa.Int64Array.from_buffers(pa.int64(),
                                       length, [None, buffer],
                                       null_count=0))

    batch = pa.record_batch(arrays, schema)
    batches = []

    records = 0
    while records < total_records:
        if records + length > total_records:
            last_length = total_records - records
            batches.append(batch.slice(0, last_length))
            records += last_length
        else:
            batches.append(batch)
            records += length

    return batches, schema


def make_reader(schema, batches):
    return pa.RecordBatchReader.from_batches(schema, batches)


def generate_batches(schema, reader):
    with io.BytesIO() as sink, pa.ipc.new_stream(sink, schema) as writer:
        for batch in reader:
            sink.seek(0)
            sink.truncate(0)
            writer.write_batch(batch)
            yield sink.getvalue()

        sink.seek(0)
        sink.truncate(0)
        writer.close()
        yield sink.getvalue()


class PostRequestServer(BaseHTTPRequestHandler):

    def do_POST(self):
        data = self.rfile.read(int(
            self.headers['Content-Length'])).decode('ascii')
        params = urllib.parse.parse_qs(data)

        self.close_connection = True

        if 'n_records' not in params.keys():
            # send invalid response
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            message = "Invalid request. Please provide 'n_records' parameter"
            self.wfile.write(bytes(message, "utf8"))

        # the parse data returned by urllib.parse.parse_qs is a dictionary
        # where each key is query variable name and the values are list of
        # values for each name. Here, params['n_records'][0] wil be the
        # first value in the list of parsed values of query data.
        n_records = int(params['n_records'][0])

        self.send_response(200)
        self.send_header('Content-type', 'application/vnd.apache.arrow.stream')

        if self.request_version == 'HTTP/1.0':
            self.protocol_version = 'HTTP/1.0'
            chunked = False
        else:
            # If HTTP/1.1, use chunked encoding
            chunked = True
            self.send_header('Transfer-Encoding', 'chunked')

        self.end_headers()

        batches, schema = get_post_data_and_schema(n_records)

        for buffer in generate_batches(schema, make_reader(schema, batches)):
            if chunked:
                self.wfile.write('{:X}\r\n'.format(
                    len(buffer)).encode('utf-8'))
            self.wfile.write(buffer)
            if chunked:
                self.wfile.write('\r\n'.encode('utf-8'))
            self.wfile.flush()

        if chunked:
            self.wfile.write('0\r\n\r\n'.encode('utf-8'))
            self.wfile.flush()


server_address = ('localhost', 8008)
try:
    httpd = HTTPServer(server_address, PostRequestServer)
    print(f'Serving on {server_address[0]}:{server_address[1]}...')
    httpd.serve_forever()
except KeyboardInterrupt:
    print('Shutting down server')
    httpd.socket.close()

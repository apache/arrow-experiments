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
from random import randbytes
from http.server import BaseHTTPRequestHandler, HTTPServer
import io

# use chunked transfer encoding?
chunked_encoding = True

schema = pa.schema([
    ('a', pa.int64()),
    ('b', pa.int64()),
    ('c', pa.int64()),
    ('d', pa.int64())
])

def GetPutData():
    total_records = 100000000
    length = 4096
    ncolumns = 4
    
    arrays = []
    
    for x in range(0, ncolumns):
        buffer = pa.py_buffer(randbytes(length * 8))
        arrays.append(pa.Int64Array.from_buffers(pa.int64(), length, [None, buffer], null_count=0))
    
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
    
    return batches

def generate_buffers(schema, source):
    with io.BytesIO() as sink, pa.ipc.new_stream(sink, schema) as writer:
        for batch in source:
            sink.seek(0)
            writer.write_batch(batch)
            sink.truncate()
            with sink.getbuffer() as buffer:
                yield buffer
        
        sink.seek(0)
        writer.close()
        sink.truncate()
        with sink.getbuffer() as buffer:
            yield buffer

# def chunk_huge_buffer(view, max_chunk_size):
#     if len(view) <= max_chunk_size:
#         yield view
#         return
#     num_splits = len(view) // max_chunk_size
#     for i in range(num_splits):
#         with view[i * max_chunk_size:i * max_chunk_size + max_chunk_size] as chunk:
#             yield chunk
#     last_chunk_size = len(view) - (num_splits * max_chunk_size)
#     if last_chunk_size > 0:
#         with view[num_splits * max_chunk_size:] as chunk:
#             yield chunk

# def generate_chunked_buffers(schema, source, max_chunk_size):
#     for buffer in generate_buffers(schema, source):
#         with memoryview(buffer) as view:
#             for chunk in chunk_huge_buffer(view, max_chunk_size):
#                 yield chunk
 
class MyServer(BaseHTTPRequestHandler):
    def resolve_batches(self):
        return pa.RecordBatchReader.from_batches(schema, batches)

    def do_GET(self):
        ### given a source of record batches, this function sends them
        ### to a client using HTTP chunked transfer encoding.
        source = self.resolve_batches()

        if self.request_version == 'HTTP/1.0':
            self.protocol_version = 'HTTP/1.0'
            chunked = False
        else:
            self.protocol_version = 'HTTP/1.1'
            chunked = chunked_encoding
        
        self.close_connection = True
        self.send_response(200)
        self.send_header('Content-Type', 'application/vnd.apache.arrow.stream')
        
        ### set these headers if testing with a local browser-based client:
        #self.send_header('Access-Control-Allow-Origin', 'http://localhost:8008')
        #self.send_header('Access-Control-Allow-Methods', 'GET')
        #self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        
        ### set this header to make browsers download the file with a name and extension:
        #self.send_header('Content-Disposition', 'attachment; filename="data.arrows"')
        
        if chunked:
            self.send_header('Transfer-Encoding', 'chunked')
        
        self.end_headers()
        
        ### if any record batch could be larger than 2 GB, Python's
        ### http.server will error when calling self.wfile.write(),
        ### so you will need to split them into smaller chunks by
        ### using the generate_chunked_buffers() function instead
        ### if generate_buffers().
        # for buffer in generate_chunked_buffers(schema, source, int(2e9)):
        for buffer in generate_buffers(schema, source):
            if chunked:
                self.wfile.write('{:X}\r\n'.format(len(buffer)).encode('utf-8'))
            self.wfile.write(buffer)
            if chunked:
                self.wfile.write('\r\n'.encode('utf-8'))
            self.wfile.flush()
        
        if chunked:
            self.wfile.write('0\r\n\r\n'.encode('utf-8'))
            self.wfile.flush()

batches = GetPutData()

server_address = ('localhost', 8008)
try:
    httpd = HTTPServer(server_address, MyServer)
    print(f'Serving on {server_address[0]}:{server_address[1]}...')
    httpd.serve_forever()
except KeyboardInterrupt:
    print('Shutting down server')
    httpd.socket.close()

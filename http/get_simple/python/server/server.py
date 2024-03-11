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
 
class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):

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
        #self.send_header('Access-Control-Allow-Origin', 'http://localhost:8000')
        #self.send_header('Access-Control-Allow-Methods', 'GET')
        #self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        
        ### set this header to make browsers download the file with a name and extension:
        #self.send_header('Content-Disposition', 'attachment; filename="data.arrows"')
        
        if chunked:
            self.send_header('Transfer-Encoding', 'chunked')
        
        self.end_headers()
        
        for buffer in generate_batches(schema, make_reader(schema, batches)):
            if chunked:
                self.wfile.write('{:X}\r\n'.format(len(buffer)).encode('utf-8'))
            self.wfile.write(buffer)
            if chunked:
                self.wfile.write('\r\n'.encode('utf-8'))
            self.wfile.flush()
            
            ### if any record batch could be larger than 2 GB, Python's
            ### http.server will error when calling self.wfile.write(),
            ### so you will need to split them into smaller chunks by 
            ### replacing the six lines above with this:
            #chunk_size = int(2e9)
            #chunk_splits = len(buffer) // chunk_size
            #for i in range(chunk_splits):
            #    if chunked:
            #        self.wfile.write('{:X}\r\n'.format(chunk_size).encode('utf-8'))
            #    self.wfile.write(buffer[i * chunk_size:i * chunk_size + chunk_size])
            #    if chunked:
            #        self.wfile.write('\r\n'.encode('utf-8'))
            #    self.wfile.flush()
            #last_chunk_size = len(buffer) - (chunk_splits * chunk_size)
            #if chunked:
            #    self.wfile.write('{:X}\r\n'.format(last_chunk_size).encode('utf-8'))
            #self.wfile.write(buffer[chunk_splits * chunk_size:])
            #if chunked:
            #    self.wfile.write('\r\n'.encode('utf-8'))
            #self.wfile.flush()
        
        if chunked:
            self.wfile.write('0\r\n\r\n'.encode('utf-8'))
            self.wfile.flush()

batches = GetPutData()

server_address = ('localhost', 8000)
try:
    httpd = HTTPServer(server_address, MyServer)
    print(f'Serving on {server_address[0]}:{server_address[1]}...')
    httpd.serve_forever()
except KeyboardInterrupt:
    print('Shutting down server')
    httpd.socket.close()

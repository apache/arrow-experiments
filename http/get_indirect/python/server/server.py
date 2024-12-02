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

from http.server import SimpleHTTPRequestHandler, HTTPServer
import json
import os
import mimetypes

mimetypes.add_type("application/vnd.apache.arrow.stream", ".arrows")

class MyServer(SimpleHTTPRequestHandler):
    def list_directory(self, path):
        host, port = self.server.server_address

        try:
            file_paths = [
                f for f in os.listdir(path)
                if f.endswith(".arrows") and os.path.isfile(os.path.join(path, f))
            ]
        except OSError:
            self.send_error(404, "No permission to list directory")
            return None

        file_uris = [f"http://{host}:{port}{self.path}{f}" for f in file_paths]
        uris_doc = {"arrow_stream_files": [{"uri": f} for f in file_uris]}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(uris_doc, indent=4).encode("utf-8"))
        return None

server_address = ("localhost", 8008)
try:
    httpd = HTTPServer(server_address, MyServer)
    print(f"Serving on {server_address[0]}:{server_address[1]}...")
    httpd.serve_forever()
except KeyboardInterrupt:
    print("Shutting down server")
    httpd.socket.close()

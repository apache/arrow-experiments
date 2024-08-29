<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# HTTP GET Arrow Data in multipart/mixed: Python Client Example

This directory contains an example of a Python HTTP server that sends a
`multipart/mixed` response to clients. The server:
1. Creates a list of record batches and populates it with synthesized data.
2. Listens for HTTP GET requests from clients.
3. Upon receiving a request, builds and sends an HTTP 200 `multipart/mixed`
   response containing:
   - A JSON part with metadata about the Arrow stream.
   - An Arrow stream part with the Arrow IPC stream of record batches.
   - A plain text part with a message containing timing information. This part
     is optional (included if `?include_footnotes` is present in the URL).

To run this example:

```sh
pip install pyarrow
python server.py
```

> [!NOTE]  
> This example uses Python's built-in
> [`http.server`](https://docs.python.org/3/library/http.server.html) module.
> This allows us to implement [chunked transfer
> encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) manually.
> Other servers may implement chunked transfer encoding automatically at the
> cost of an undesirable new layer of buffering. Arrow IPC streams already offer
> a natural way of chunking large amounts of tabular data. It's not a general
> requirement, but in this example each chunk corresponds to one Arrow record
> batch.

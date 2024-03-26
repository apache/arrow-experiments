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

# HTTP GET Arrow Data: Simple Python Server Example

This directory contains a minimal example of an HTTP server implemented in Python. The server:
1. Creates a list of record batches and populates it with synthesized data.
2. Listens for HTTP POST requests from clients.
3. Upon receiving a request, sends an HTTP 200 response with the body containing an Arrow IPC stream of record batches.

To run this example:

```sh
pip install pyarrow
python server.py
```

> [!NOTE]  
> This example uses Python's built-in [`http.server`](https://docs.python.org/3/library/http.server.html) module. This server does not implement chunked transfer encoding automatically like more sophisticated HTTP servers do, so this example implements it manually, with each chunk consisting of one Arrow record batch. Note that in servers that implement chunked transfer encoding automatically, each chunk will generally not correspond to one Arrow record batch.

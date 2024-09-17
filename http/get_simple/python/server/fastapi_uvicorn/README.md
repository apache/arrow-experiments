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

# HTTP GET Arrow Data: Simple Python Server Example with FastAPI and Uvicorn

This directory contains a minimal example of an HTTP server implemented in Python using the [FastAPI](https://fastapi.tiangolo.com) framework and the [Uvicorn](https://www.uvicorn.org) web server. This example:
1. Creates a list of record batches and populates it with synthesized data.
2. Listens for HTTP GET requests from clients.
3. Upon receiving a request, sends an HTTP 200 response with the body containing an Arrow IPC stream of record batches.

To run this example:

```sh
pip install fastapi
pip install "uvicorn[standard]"
pip install pyarrow
uvicorn server:app --port 8008
```

> [!NOTE]
> This example requires Starlette 0.38.0 or newer, which added support for `memoryview` in `StreamingResponse`. If using an older version of Starlette, change both instances of:
> ```py
> with sink.getbuffer() as buffer:
>     yield buffer
> ```
> to:
> ```py
> yield sink.getvalue()
> ```

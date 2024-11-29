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

# HTTP GET Arrow Data: Indirect Python Server Example

This directory contains an example of an HTTP server implemented in Python using the built-in [`http.server`](https://docs.python.org/3/library/http.server.html) module. The server:
1. Listens for HTTP GET requests from clients.
2. Upon receiving a GET request for the document root, serve a JSON document that lists the filenames of all the `.arrows` files in the current directory.
3. Upon receiving a GET request for a specific `.arrows` file, serve that file.

To run this example, first copy two `.arrows` files from the `data` section of this repository into the current directory:

```sh
cp ../../../../data/arrow-commits/arrow-commits.arrows .
cp ../../../../data/rand-many-types/random.arrows .
```

Then start the HTTP server:

```sh
python server.py
```

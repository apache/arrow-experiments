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

# HTTP GET Arrow Data: Simple Swift Server Example

This directory contains a minimal example of an HTTP server implemented in Swift. The server:

1. Creates a record batches and populates it with synthesized data.
2. Listens for HTTP requests from clients.
3. Upon receiving a request, sends an HTTP 200 response with the body containing an Arrow IPC stream of record batches.
To run this example:

```sh
git clone --filter=blob:none --no-checkout --depth 1 --sparse https://github.com/apache/arrow.git
pushd arrow
git sparse-checkout add swift/Arrow
git checkout
popd
mkdir -p vendor/Arrow
mv arrow/swift/Arrow vendor
rm -rf arrow
```

2. run:

```sh
swift run 
```
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

# HTTP GET Arrow Data: Simple Swift Client Example

This directory contains a minimal example of an HTTP client implemented in Swift. The client:
1. Sends an HTTP GET request to a server.
2. Receives an HTTP 200 response from the server, with the response body containing an Arrow IPC stream record batch.
3. Prints some of the record batches attributes and data to the terminal

To run this example, first start one of the server examples in the parent directory, then:
1. download and copy Apache Arrow Swift's Arrow folder into the vendor/Arrow folder:

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
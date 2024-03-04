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

# HTTP GET Arrow Data: Simple C++ Client Example

This directory contains a minimal example of an HTTP client implemented in C++. The client:
1. Sends an HTTP GET request to a server.
2. Receives an HTTP 200 response from the server, with the response body containing an Arrow IPC stream of record batches.
3. Collects the record batches as they are received.

To run this example, first start one of the server examples in the parent directory. Then install the `arrow` and `libcurl` C++ libraries, compile `client.cpp`, and run the executable. For example, using `clang++`:

```sh
clang++ client.cpp -std=c++17 $(pkg-config --cflags --libs arrow libcurl) -o client
./client
```

> [!NOTE]
> The example here requires version 15.0.0 or higher of the Arrow C++ library because of a bug ([#39163](https://github.com/apache/arrow/issues/39163)) that existed in earlier versions. If you must use an earlier version of the Arrow C++ library, it is possible to implement an HTTP client by using `arrow::ipc::RecordBatchStreamReader` instead of `arrow::ipc::StreamDecoder`. See [this example](https://github.com/apache/arrow/pull/39081/commits/3b937b98295b5dd4f9e297a865a9303a317c9983) for reference.

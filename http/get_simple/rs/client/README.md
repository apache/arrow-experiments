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

# HTTP GET Arrow Data: Simple Rust Client Example

This directory contains a minimal example of an HTTP client implemented in Rust. The client:

1. Sends an HTTP GET request to a server.
2. Receives an HTTP 200 response from the server, with the response body containing an Arrow IPC stream of record batches.
3. Adds the record batches to a list as they are received.

To run this example, first start one of the server examples in the parent directory, then:

```sh
cargo r --release
```
> [!NOTE]  
> This client example implements low-level HTTP/1.1 details directly, instead of using an HTTP library. We intend to update the example to use [hyper](https://docs.rs/hyper/latest/hyper/) after [arrow-rs has an async Arrow IPC reader](https://github.com/apache/arrow-rs/issues/1207)).

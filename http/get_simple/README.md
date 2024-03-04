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

# HTTP GET Arrow Data: Simple Examples

This directory contains a set of minimal examples of HTTP clients and servers implemented in several languages. These examples demonstrate:
- How a client can send a GET request to a server and receive a response from the server containing an Arrow IPC stream of record batches.
- How a server can respond to a GET request from a client and send the client a response containing an Arrow IPC stream of record batches.

To enable performance comparisons to Arrow Flight RPC, the server examples generate the data in exactly the same way as in [`flight_benchmark.cc`](https://github.com/apache/arrow/blob/7346bdffbdca36492089f6160534bfa2b81bad90/cpp/src/arrow/flight/flight_benchmark.cc#L194-L245) as cited in the [original blog post introducing Flight RPC](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/). But note that Flight example sends four concurrent streams.

If you are collaborating on the set of examples in this directory, please follow these guidelines:
- Each new example must be implemented as minimally as possible. For example, error handling should be minimized or omitted.
- Each new client example must be tested to ensure that it works with each existing server example.
- Each new server example must be tested to ensure that it works with each existing client example.
- To the greatest extent possible, each new server example should be functionally equivalent to each existing server example (generating equivalent data with the same schema, size, shape, and distribution of values; sending the same HTTP headers; and so on).
- Each new client example must print timing and size information before exiting. At a minimum this must include the number of seconds elapsed (rounded to the second decimal place) and the number of record batches received.

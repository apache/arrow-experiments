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

# Apache Arrow HTTP Data Transport

This area of the Apache Arrow Experiments repository is for collaborative prototyping and research on the subject of sending and receiving data in Arrow IPC stream format (IANA media type `application/vnd.apache.arrow.stream`) over HTTP APIs.

The subdirectories beginning with **get** demonstrate clients receiving data from servers (HTTP GET request). Those beginning with **post** demonstrate clients sending data to servers (. The contents of the subdirectories are as follows:
- **[get_compressed](get_compressed)** demonstrates various ways of using compression.
- **[get_indirect](get_indirect)** demonstrates a two-step sequence of receiving Arrow data, in which a JSON document provides the URIs for the Arrow data.
- **[get_multipart](get_multipart)** demonstrates how to send and receive a multipart HTTP response (`multipart/mixed`) containing Arrow IPC stream data and other data.
- **[get_range](get_range)** demonstrates how to use HTTP range requests to download Arrow IPC stream data of known length in multiple requests.
- **[get_simple](get_simple)** contains a large set of examples demonstrating the basics of fetching an Arrow IPC stream from a server to a client in 12+ languages.
- **[post_multipart](post_multipart)** demonstrates how to send and receive a multipart HTTP request body (`multipart/mixed`) containing Arrow IPC stream data and other data.
- **[post_simple](post_simple)** demonstrates the basics of sending Arrow IPC stream data from a client to a server.


The intent of this work is to:
- Ensure excellent interoperability across languages.
- Allow implementation within existing HTTP APIs.
- Maximize performance.
- Minimize implementation complexity.

The end goal of this work is to inform and guide the creation of a set of conventions to be published in the Arrow documentation.

> [!IMPORTANT]
> Before contributing to this area of the repository, please see the [related discussion on the Arrow developer mailing list](https://lists.apache.org/thread/vfz74gv1knnhjdkro47shzd1z5g5ggnf) and the [Arrow GitHub issue listing the tasks that are a part of this effort](https://github.com/apache/arrow/issues/40465).

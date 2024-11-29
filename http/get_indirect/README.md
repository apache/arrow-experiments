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

# HTTP GET Arrow Data: Indirect Examples

This directory contains examples of HTTP clients and servers that use a two-step sequence to retrieve Arrow data:
1. The client sends a GET request to a server and receives a JSON response from the server containing one or more server URIs.
2. The client sends GET requests to each of those URIs and receives a response from each server containing an Arrow IPC stream of record batches (exactly as in the [simple GET examples](https://github.com/apache/arrow-experiments/tree/main/http/get_simple)).

> [!IMPORTANT]  
> The structure of the JSON document in these examples is provided for example purposes only. Developers should use different JSON document structures in their applications.

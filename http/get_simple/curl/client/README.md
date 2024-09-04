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

# HTTP GET Arrow Data: Simple curl Client Example

This directory contains a simple `curl` command that:
1. Sends an HTTP GET request to a server.
2. Receives an HTTP 200 response from the server, with the response body containing an Arrow IPC stream of record batches.
3. Writes the stream of record batches to an Arrow IPC stream file with filename `output.arrows`.

To run this example, first start one of the server examples in the parent directory, then run the `curl` command.

### Reading the Resulting Arrow IPC Stream File

To read the resulting file `output.arrows` and retrieve the schema and record batches that it contains, you can use one of the code examples below, or use similar examples in other languages that have Arrow implementations. You can also read the file with any application that supports reading data in the Arrow IPC streaming format.

<details>
  <summary>Example: Read Arrow IPC stream file with Python</summary>

  ```py
  import pyarrow as pa

  with open("output.arrows", "rb") as f:
      reader = pa.ipc.open_stream(pa.BufferReader(f.read()))

  schema = reader.schema

  batch = reader.read_next_batch()
  # ...

  # or alternatively:
  batches = [b for b in reader]
  ```
</details>

<details>
  <summary>Example: Read Arrow IPC stream file with R</summary>

  ```r
  library(arrow)

  reader <- RecordBatchStreamReader$create(ReadableFile$create("output.arrows"))

  schema <- reader$schema

  batch <- reader$read_next_batch()
  # ...

  # or alternatively:
  table <- reader$read_table()
  ```
</details>

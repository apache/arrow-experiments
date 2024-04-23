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

# Arrow Dissociated IPC Protocol Example

This directory contains a reference example implementation of the
[Arrow Dissociated IPC Protocol](https://arrow.apache.org/docs/dev/format/DissociatedIPC.html).

This protocol splits the Arrow Flatbuffers IPC metadata and the body buffers
into separate streams to allow for utilizing shared memory, non-cpu device
memory, or remote memory (RDMA) with Arrow formatted datasets.

This example utilizes [libcudf](https://docs.rapids.ai/api) and
[UCX](https://openucx.readthedocs.io/en/master/#) to transfer Arrow data
located on an NVIDIA GPU.

## Building

You must have libcudf, libarrow, libarrow_flight, libarrow_cuda, and ucx
accessible on your `CMAKE_MODULE_PATH`/`CMAKE_PREFIX_PATH` so that `cmake` can find them.

After that you can simply do the following:

```console
$ mkdir build && cd build
$ cmake ..
$ make
```

to build the `arrow-cudf-flight` mainprog.

## Running

You can start the server by just running `arrow-cudf-flight` which will
default to using `31337` as the Flight port and `127.0.0.1` for the host.
Both of these can be changed via the `-port` and `-address` gflags
respectively.

You can run the client by adding the `-client` option when running the
command.

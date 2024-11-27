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

# HTTP GET Arrow Data: Compressed Arrow Data Examples

This directory contains a simple `curl` script that issues multiple HTTP GET
requests to the server implemented in the parent directory, negotiating
different compression algorithms for the Arrow IPC stream data piping the output
to different files with extensions that indicate the compression algorithm used.

To run this example, first start one of the server examples in the parent
directory, then run the `client.sh` script.

You can check all the sizes with a simple command:

```bash
$ du -sh out* | sort -gr
816M    out.arrows
804M    out_from_chunked.arrows
418M    out_from_chunked.arrows+lz4
405M    out.arrows+lz4
257M    out.arrows.gz
256M    out_from_chunked.arrows.gz
229M    out_from_chunked.arrows+zstd
229M    out.arrows+zstd
220M    out.arrows.zstd
219M    out_from_chunked.arrows.zstd
 39M    out_from_chunked.arrows.br
 38M    out.arrows.br
```

> [!WARNING]
> Better compression is not the only relevant metric as it might come with a
> trade-off in terms of CPU usage. The best compression algorithm for your use
> case will depend on your specific requirements.

## Meaning of the file extensions

Files produced by HTTP/1.0 requests are not chunked, they get buffered in memory
at the server before being sent to the client. If compressed, they end up
slightly smaller than the results of chunked responses, but the extra delay for
first byte is not worth it in most cases.

 - `out.arrows` (Uncompressed)
 - `out.arrows.gz` (Gzip HTTP compression)
 - `out.arrows.zstd` (Zstandard HTTP compression)
 - `out.arrows.br` (Brotli HTTP compression)

 - `out.arrows+zstd` (Zstandard IPC compression)
 - `out.arrows+lz4` (LZ4 IPC compression)

HTTP/1.1 requests are returned by the server with `Transfer-Encoding: chunked`
to send the data in smaller chunks that are sent to the socket as soon as they
are ready. This is useful for large responses that take a long time to generate
at the cost of a small overhead caused by the independent compression of each
chunk.

 - `out_from_chunked.arrows` (Uncompressed)
 - `out_from_chunked.arrows.gz` (Gzip HTTP compression)
 - `out_from_chunked.arrows.zstd` (Zstandard HTTP compression)
 - `out_from_chunked.arrows.br` (Brotli HTTP compression)

 - `out_from_chunked.arrows+lz4` (LZ4 IPC compression)
 - `out_from_chunked.arrows+zstd` (Zstandard IPC compression)

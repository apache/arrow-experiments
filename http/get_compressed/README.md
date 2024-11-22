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

# HTTP GET Arrow Data: Compression Examples

This directory contains examples of HTTP servers/clients that transmit/receive
data in the Arrow IPC streaming format and use compression (in various ways) to
reduce the size of the transmitted data.

Since we re-use the [Arrow IPC format][ipc] for transferring Arrow data over
HTTP and both Arrow IPC and HTTP standards support compression on their own,
there are at least two approaches to this problem:

1. Compressed HTTP responses carrying Arrow IPC streams with uncompressed
   array buffers.
2. Uncompressed HTTP responses carrying Arrow IPC streams with compressed
   array buffers.

Applying both IPC buffer and HTTP compression to the same data is not
recommended. The extra CPU overhead of decompressing the data twice is
not worth any possible gains that double compression might bring. If
compression ratios are unambiguously more important than reducing CPU
overhead, then a different compression algorithm that optimizes for that can
be chosen.

This table shows the support for different compression algorithms in HTTP and
Arrow IPC:

| Codec      | Identifier  | HTTP Support  | IPC Support  |
|----------- | ----------- | ------------- | ------------ |
| GZip       | `gzip`      | X             |              |
| DEFLATE    | `deflate`   | X             |              |
| Brotli     | `br`        | X[^2]         |              |
| Zstandard  | `zstd`      | X[^2]         | X[^3]        |
| LZ4        | `lz4`       |               | X[^3]        |

Since not all Arrow IPC implementations support compression, HTTP compression
based on accepted formats negotiated with the client is a great way to increase
the chances of efficient data transfer.

Servers may check the `Accept-Encoding` header of the client and choose the
compression format in this order of preference: `zstd`, `br`, `gzip`,
`identity` (no compression). If the client does not specify a preference, the
only constraint on the server is the availability of the compression algorithm
in the server environment.

## Arrow IPC Compression

When IPC buffer compression is preferred and servers can't assume all clients
support it[^4], clients may be asked to explicitly list the supported compression
algorithms in the request headers. The `Accept` header can be used for this
since `Accept-Encoding` (and `Content-Encoding`) is used to control compression
of the entire HTTP response stream and instruct HTTP clients (like browsers) to
decompress the response before giving data to the application or saving the
data.

    Accept: application/vnd.apache.arrow.stream; codecs="zstd, lz4"

This is similar to clients requesting video streams by specifying the
container format and the codecs they support
(e.g. `Accept: video/webm; codecs="vp8, vorbis"`).

The server is allowed to choose any of the listed codecs, or not compress the
IPC buffers at all. Uncompressed IPC buffers should always be acceptable by
clients.

If a server adopts this approach and a client does not specify any codecs in
the `Accept` header, the server can fall back to checking `Accept-Encoding`
header to pick a compression algorithm for the entire HTTP response stream.

To make debugging easier servers may include the chosen compression codec(s)
in the `Content-Type` header of the response (quotes are optional):

    Content-Type: application/vnd.apache.arrow.stream; codecs=zstd

This is not necessary for correct decompression because the payload already
contains information that tells the IPC reader how to decompress the buffers,
but it can help developers understand what is going on.

When programatically checking if the `Content-Type` header contains a specific
format, it is important to use a parser that can handle parameters or look
only at the media type part of the header. This is not an exclusivity of the
Arrow IPC format, but a general rule for all media types. For example,
`application/json; charset=utf-8` should match `application/json`.

When considering use of IPC buffer compression, check the [IPC format section of
the Arrow Implementation Status page][^5] to see whether the the Arrow
implementations you are targeting support it.

## HTTP/1.1 Response Compression

HTTP/1.1 offers an elaborate way for clients to specify their preferred
content encoding (read compression algorithm) using the `Accept-Encoding`
header.[^1]

At least the Python server (in [`python/`](./python)) implements a fully
compliant parser for the `Accept-Encoding` header. Application servers may
choose to implement a simpler check of the `Accept-Encoding` header or assume
that the client accepts the chosen compression scheme when talking to that
server.

Here is an example of a header that a client may send and what it means:

    Accept-Encoding: zstd;q=1.0, gzip;q=0.5, br;q=0.8, identity;q=0

This header says that the client prefers that the server compress the
response with `zstd`, but if that is not possible, then `brotli` and `gzip`
are acceptable (in that order because 0.8 is greater than 0.5). The client
does not want the response to be uncompressed. This is communicated by
`"identity"` being listed with `q=0`.

To tell the server the client only accepts `zstd` responses and nothing
else, not even uncompressed responses, the client would send:

    Accept-Encoding: zstd, *;q=0

RFC 2616[^1] specifies the rules for how a server should interpret the
`Accept-Encoding` header:

    A server tests whether a content-coding is acceptable, according to
    an Accept-Encoding field, using these rules:

       1. If the content-coding is one of the content-codings listed in
          the Accept-Encoding field, then it is acceptable, unless it is
          accompanied by a qvalue of 0. (As defined in section 3.9, a
          qvalue of 0 means "not acceptable.")

       2. The special "*" symbol in an Accept-Encoding field matches any
          available content-coding not explicitly listed in the header
          field.

       3. If multiple content-codings are acceptable, then the acceptable
          content-coding with the highest non-zero qvalue is preferred.

       4. The "identity" content-coding is always acceptable, unless
          specifically refused because the Accept-Encoding field includes
          "identity;q=0", or because the field includes "*;q=0" and does
          not explicitly include the "identity" content-coding. If the
          Accept-Encoding field-value is empty, then only the "identity"
          encoding is acceptable.

If you're targeting web browsers, check the compatibility table of [compression
algorithms on MDN Web Docs][^2].

Another important rule is that if the server compresses the response, it
must include a `Content-Encoding` header in the response.

    If the content-coding of an entity is not "identity", then the
    response MUST include a Content-Encoding entity-header (section
    14.11) that lists the non-identity content-coding(s) used.

Since not all servers implement the full `Accept-Encoding` header parsing logic,
clients tend to stick to simple header values like `Accept-Encoding: identity`
when no compression is desired, and `Accept-Encoding: gzip, deflate, zstd, br`
when the client supports different compression formats and is indifferent to
which one the server chooses. Clients should expect uncompressed responses as
well in theses cases. The only way to force a "406 Not Acceptable" response when
no compression is available is to send `identity;q=0` or `*;q=0` somewhere in
the end of the `Accept-Encoding` header. But that relies on the server
implementing the full `Accept-Encoding` handling logic.


[^1]: [Fielding, R. et al. (1999). HTTP/1.1. RFC 2616, Section 14.3 Accept-Encoding.](https://www.rfc-editor.org/rfc/rfc2616#section-14.3)
[^2]: [MDN Web Docs: Accept-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding#browser_compatibility)
[^3]: [Arrow Columnar Format: Compression](https://arrow.apache.org/docs/format/Columnar.html#compression)
[^4]: Web applications using the JavaScript Arrow implementation don't have
    access to the compression APIs to decompress `zstd` and `lz4` IPC buffers.
[^5]: [Arrow Implementation Status: IPC Format](https://arrow.apache.org/docs/status.html#ipc-format)

[ipc]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

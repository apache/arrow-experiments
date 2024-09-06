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

This directory contains examples of HTTP servers/clients that transmit/receive data in the Arrow IPC streaming format and use compression (in various ways) to reduce the size of the transmitted data.

## HTTP/1.1 Response Compression

HTTP/1.1 offers an elaborate way for clients to specify their preferred
content encoding (read compression algorithm) using the `Accept-Encoding`
header.[^1]

At least the Python server (in `python/`)  implements a fully compliant
parser for the `Accept-Encoding` header. Application servers may choose
to implement a simpler check of the `Accept-Encoding` header or assume
that the client accepts the chosen compression scheme when talking
to that server.

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


[^1]: [Fielding, R. et al. (1999). HTTP/1.1. RFC 2616, Section 14.3 Accept-Encoding.](https://www.rfc-editor.org/rfc/rfc2616#section-14.3)
[^2]: [MDN Web Docs: Accept-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding#browser_compatibility)


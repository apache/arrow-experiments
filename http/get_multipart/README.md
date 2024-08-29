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

# HTTP GET Arrow Data: Multipart Examples

This directory contains examples of HTTP servers/clients that send/receive a multipart response (`Content-Type: multipart/mixed`) containing JSON data (`Content-Type: application/json`), an Arrow IPC stream data (`Content-Type: application/vnd.apache.arrow.stream`), and (optionally) plain text data (`Content-Type: text/plain`).

## Picking a Boundary

The `multipart/mixed` response format uses a boundary string to separate the
parts. This string **must not appear in the content of any part**
(RFC 1341[^1]).

We **do not recommend** checking for the boundary string in the content of the
parts as that would prevent streaming them. Which would add up to the memory
usage of the server and waste CPU time.

### Recommended Algorithm

For every `multipart/mixed` response produced by the server:
1. Using a CSPRNG[^2], generate a byte string of enough entropy to make the
   probability of collision[^3] negligible (at least 160 bits = 20 bytes)[^4].
2. Encode the byte string in a way that is safe to use in HTTP headers. We
   recommend using `base64url` encoding described in RFC 4648[^5].

`base64url` encoding is a variant of `base64` encoding that uses `-` and `_`
instead of `+` and `/` respectively. It also omits padding characters (`=`).

This algorithm can be implemented in Python using the `secret.token_urlsafe()`
function.

If you generate a boundary string with generous 224 bits of entropy
(i.e. 28 bytes), the base64url encoding will produce a 38-character
string which is well below the limit defined by RFC 1341 (70 characters).

    >>> import secrets
    >>> boundary = secrets.token_urlsafe(28)
    >>> len(boundary)
    38


[^1]: [RFC 1341 - Section 7.2 The Multipart Content-Type](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html)
[^2]: [Cryptographically Secure Pseudo-Random Number Generator](https://en.wikipedia.org/wiki/Cryptographically_secure_pseudorandom_number_generator)
[^3]: [Birthday Problem](https://en.wikipedia.org/wiki/Birthday_problem)
[^4]: [Hash Collision Probabilities](https://preshing.com/20110504/hash-collision-probabilities/)
[^5]: [RFC 4648 - Section 5 Base 64 Encoding with URL and Filename Safe Alphabet](https://tools.ietf.org/html/rfc4648#section-5)

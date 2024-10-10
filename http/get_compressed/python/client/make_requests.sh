#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

CURL="curl --verbose"
URI="http://localhost:8008"
OUT_HTTP1=out.arrows
OUT_CHUNKED=out_from_chunked.arrows

# HTTP/1.0 means that response is not chunked and not compressed...
$CURL --http1.0 -o $OUT_HTTP1 $URI
# ...but it may be compressed with an explicitly set Accept-Encoding
# header
$CURL --http1.0 -H "Accept-Encoding: gzip, *;q=0" -o $OUT_HTTP1.gz $URI
$CURL --http1.0 -H "Accept-Encoding: zstd, *;q=0" -o $OUT_HTTP1.zstd $URI
$CURL --http1.0 -H "Accept-Encoding: br, *;q=0" -o $OUT_HTTP1.br $URI
# ...or with IPC buffer compression if the Accept header specifies codecs.
$CURL --http1.0 -H "Accept: application/vnd.apache.arrow.stream; codecs=\"zstd, lz4\"" -o $OUT_HTTP1+zstd $URI
$CURL --http1.0 -H "Accept: application/vnd.apache.arrow.stream; codecs=lz4" -o $OUT_HTTP1+lz4 $URI

# HTTP/1.1 means compression is on by default...
# ...but it can be refused with the Accept-Encoding: identity header.
$CURL -H "Accept-Encoding: identity" -o $OUT_CHUNKED $URI
# ...with gzip if no Accept-Encoding header is set.
$CURL -o $OUT_CHUNKED.gz $URI
# ...or with the compression algorithm specified in the Accept-Encoding.
$CURL -H "Accept-Encoding: zstd, *;q=0" -o $OUT_CHUNKED.zstd $URI
$CURL -H "Accept-Encoding: br, *;q=0" -o $OUT_CHUNKED.br $URI
# ...or with IPC buffer compression if the Accept header specifies codecs.
$CURL -H "Accept: application/vnd.apache.arrow.stream; codecs=\"zstd, lz4\"" -o $OUT_CHUNKED+zstd $URI
$CURL -H "Accept: application/vnd.apache.arrow.stream; codecs=lz4" -o $OUT_CHUNKED+lz4 $URI

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
OUT=output.arrows
OUT2=output2.arrows

# HTTP/1.0 means that response is not chunked and not compressed...
$CURL --http1.0 -o $OUT $URI
# ...but it may be compressed with an explicitly set Accept-Encoding
# header
$CURL --http1.0 -H "Accept-Encoding: gzip" -o $OUT.gz $URI
$CURL --http1.0 -H "Accept-Encoding: zstd" -o $OUT.zstd $URI
$CURL --http1.0 -H "Accept-Encoding: br" -o $OUT.brotli $URI

# HTTP/1.1 means compression is on by default...
# ...but it can be refused with the Accept-Encoding: identity header.
$CURL -H "Accept-Encoding: identity" -o $OUT2 $URI
# ...with gzip if no Accept-Encoding header is set.
$CURL -o $OUT2.gz $URI
# ...or with the compression algorithm specified in the Accept-Encoding.
$CURL -H "Accept-Encoding: zstd" -o $OUT2.zstd $URI
$CURL -H "Accept-Encoding: br" -o $OUT2.brotli $URI

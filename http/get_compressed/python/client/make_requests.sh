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

OUT=output.arrows
URI="http://localhost:8008"
CURL="curl --verbose"

# HTTP/1.0 means that response is not chunked and not compressed...
$CURL --http1.0 -o $OUT $URI
# ...but it may be compressed with an explicitly set Accept-Encoding
# header
# $CURL --http1.0 -H "Accept-Encoding: gzip" -o $OUT $URI
$CURL --http1.0 -H "Accept-Encoding: zstd" -o $OUT.zstd $URI
$CURL --http1.0 -H "Accept-Encoding: br" -o $OUT.brotli $URI

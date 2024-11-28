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


### Use range requests to download an Arrow IPC stream file in two parts

# Get the length of the file `random.arrows` in bytes
curl -I localhost:8008/random.arrows
# Content-Length: 13550776

# Download the first half of the file to `random-part-1.arrows`
curl -r 0-6775388 localhost:8008/random.arrows -o random-part-1.arrows

# Download the second half of the file to `random-part-2.arrows`
curl -r 6775389-13550776 localhost:8008/random.arrows -o random-part-2.arrows

# Combine the two separate files into one file `random.arrows` then delete them
cat random-part-1.arrows random-part-2.arrows > random.arrows
rm random-part-1.arrows random-part-2.arrows

# Clean up
rm random.arrows


### Simulate an interrupted download over a slow connection

# Begin downloading the file at 1M/s but interrupt after five seconds
timeout 5s curl --limit-rate 1M localhost:8008/random.arrows -o random.arrows

# Resume the download at 1M/s
curl -C - --limit-rate 1M localhost:8008/random.arrows -o random.arrows

# Clean up
rm random.arrows

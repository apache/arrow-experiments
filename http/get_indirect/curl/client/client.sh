!/bin/sh

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


# Use curl to get a JSON document containing URIs to
# Arrow stream files, then use jq to extract the URIs
uris=$(curl -s -S http://localhost:8008/ | jq -r '.arrow_stream_files[].uri')

# Use curl to download the files from the URIs in parallel
if [ -n "$uris" ]; then
    curl --parallel --remote-name-all $(print $uris)
fi

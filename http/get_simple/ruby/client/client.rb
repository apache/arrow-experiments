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

require "net/http"
require "arrow"

uri = URI("http://localhost:8008")

start = Time.now
arrows_data = Net::HTTP.get(uri).freeze
table = Arrow::Table.load(Arrow::Buffer.new(arrows_data), format: :arrows)
elapsed_time = Time.now - start

n_received_record_batches = table[0].data.n_chunks
puts("#{n_received_record_batches} record batches received")
puts("%.2f seconds elapsed" % elapsed_time)

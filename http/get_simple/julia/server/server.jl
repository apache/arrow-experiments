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

using Arrow, HTTP, Tables

function get_stream(::HTTP.Request)
    total_records = 100_000_000
    batch_len = 4096
    stream = Tables.partitioner(Iterators.partition(1:total_records, batch_len)) do indices
        nrows = length(indices)
        return (
            a = rand(Int, nrows),
            b = rand(Int, nrows),
            c = rand(Int, nrows),
            d = rand(Int, nrows)
        )
    end
    buffer = IOBuffer()
    Arrow.write(buffer, stream)
    return HTTP.Response(200, take!(buffer))
end

const ARROW_ROUTER = HTTP.Router()
HTTP.register!(ARROW_ROUTER, "GET", "/", get_stream)
println("Serving on localhost:8008...")
server = HTTP.serve!(ARROW_ROUTER, "127.0.0.1", 8008)

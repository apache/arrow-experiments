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

require "open-uri"
require "stringio"
require "arrow"

host = "localhost"
port = 8008
url = URI("http://#{host}:#{port}")

# Non-streaming
resp = Net::HTTP.get(url)

StringIO.open(resp) do |stringio_input|
  Gio::RubyInputStream.open(stringio_input) do |gio_input|
    Arrow::GIOInputStream.open(gio_input) do |arrow_input|
      reader = Arrow::RecordBatchStreamReader.new(arrow_input)

      p reader.schema
      p reader.read_all
    end
  end
end

# Streaming

nrows = 0
batches = []

Net::HTTP.start(host, port) do |http|
  req = Net::HTTP::Get.new(url)

  http.request(req) do |res|
    StringIO.open(res.read_body) do |stringio_input|
      Gio::RubyInputStream.open(stringio_input) do |gio_input|
        Arrow::GIOInputStream.open(gio_input) do |arrow_input|
          reader = Arrow::RecordBatchStreamReader.new(arrow_input)

          p reader.schema

          reader.each do |batch|
            puts "Got batch of #{batch.length} rows"

            nrows += batch.length
            batches << batch
          end
        end
      end
    end
  end
end

puts "Streamed a total of #{batches.length} batches and #{nrows} total rows"

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

require "arrow"

class ArrowStreamGenerator
  def initialize(env)
    @env = env
    @schema = Arrow::Schema.new(a: :int64,
                                b: :int64,
                                c: :int64,
                                d: :int64)
    generate_record_batches
  end

  def each(&block)
    body = RackBody.new(block, need_manual_chunked?)
    Gio::RubyOutputStream.open(body) do |gio_output|
      Arrow::GIOOutputStream.open(gio_output) do |arrow_output|
        Arrow::RecordBatchStreamWriter.open(arrow_output, @schema) do |writer|
          @record_batches.each do |record_batch|
            writer.write_record_batch(record_batch)
          end
        end
      end
    end
  end

  private
  def need_manual_chunked?
    not (@env["SERVER_SOFTWARE"] || "").start_with?("WEBrick")
  end

  def generate_record_batches
    n_total_records = 100000000
    n_columns = 4

    n_rows = 4096
    max_int64 = 2 ** 63 - 1
    arrays = n_columns.times.collect do
      Arrow::Int64Array.new(n_rows.times.collect {rand(max_int64)})
    end

    record_batch = Arrow::RecordBatch.new(@schema, n_rows, arrays)
    @record_batches = [record_batch] * (n_total_records / n_rows)
    n_remained_records = n_total_records % n_rows
    if n_remained_records
      @record_batches << record_batch.slice(0, n_remained_records)
    end
  end

  class RackBody
    def initialize(block, need_manual_chunked)
      @block = block
      @need_manual_chunked = need_manual_chunked
    end

    def write(buffer)
      @block.call("#{buffer.bytesize.to_s(16)}\r\n") if @need_manual_chunked
      @block.call(buffer)
      @block.call("\r\n") if @need_manual_chunked
      buffer.bytesize
    end

    def flush
    end

    def close
      @block.call("0\r\n\r\n") if @need_manual_chunked
    end
  end
end

run do |env|
  headers = {
    "content-type" => "application/vnd.apache.arrow.stream",
    "transfer-encoding" => "chunked",
  }
  [200, headers, ArrowStreamGenerator.new(env)]
end

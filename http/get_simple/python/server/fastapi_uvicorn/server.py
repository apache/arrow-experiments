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

import pyarrow as pa
from random import randbytes
import io
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

schema = pa.schema([
    ("a", pa.int64()),
    ("b", pa.int64()),
    ("c", pa.int64()),
    ("d", pa.int64())
])


def GetPutData():
    total_records = 100000000
    length = 4096
    ncolumns = 4

    arrays = []

    for x in range(0, ncolumns):
        buffer = pa.py_buffer(randbytes(length * 8))
        arrays.append(pa.Int64Array.from_buffers(
            pa.int64(), length, [None, buffer], null_count=0))

    batch = pa.record_batch(arrays, schema)
    batches = []

    records = 0
    while records < total_records:
        if records + length > total_records:
            last_length = total_records - records
            batches.append(batch.slice(0, last_length))
            records += last_length
        else:
            batches.append(batch)
            records += length

    return batches


def generate_bytes(schema, batches):
    with pa.RecordBatchReader.from_batches(schema, batches) as source, \
            io.BytesIO() as sink, \
            pa.ipc.new_stream(sink, schema) as writer:
        for batch in source:
            sink.seek(0)
            writer.write_batch(batch)
            sink.truncate()
            with sink.getbuffer() as buffer:
                yield buffer

        sink.seek(0)
        writer.close()
        sink.truncate()
        with sink.getbuffer() as buffer:
            yield buffer


batches = GetPutData()

app = FastAPI()


@app.get("/")
def main():
    return StreamingResponse(
        generate_bytes(schema, batches),
        media_type="application/vnd.apache.arrow.stream"
    )

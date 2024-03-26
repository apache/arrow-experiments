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
import urllib.parse
from random import randbytes
import io

from fastapi import Request, FastAPI
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI()

def get_post_data_and_schema(total_records: int):
    schema = pa.schema([('a', pa.int64()), ('b', pa.int64()),
                        ('c', pa.int64()), ('d', pa.int64())])
    length = 4096
    ncolumns = 4

    arrays = []

    for x in range(0, ncolumns):
        buffer = pa.py_buffer(randbytes(length * 8))
        arrays.append(
            pa.Int64Array.from_buffers(pa.int64(),
                                       length, [None, buffer],
                                       null_count=0))

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

    return batches, schema


def make_reader(schema, batches):
    return pa.RecordBatchReader.from_batches(schema, batches)


def generate_batches(schema, reader):
    with io.BytesIO() as sink, pa.ipc.new_stream(sink, schema) as writer:
        for batch in reader:
            sink.seek(0)
            sink.truncate(0)
            writer.write_batch(batch)
            yield sink.getvalue()

        sink.seek(0)
        sink.truncate(0)
        writer.close()
        yield sink.getvalue()

@app.post("/")
async def main(request: Request):
    request_body = await request.body()
    params = urllib.parse.parse_qs(request_body.decode('ascii'))
    if 'n_records' not in params.keys():
        return JSONResponse(status_code=400, content='request failed. n_records value not found in request data.')

    n_records = int(params['n_records'][0])

    batches, schema = get_post_data_and_schema(n_records)
    def iterbuffer():
        for buffer in generate_batches(schema, make_reader(schema, batches)):
            yield buffer
    return StreamingResponse(iterbuffer())

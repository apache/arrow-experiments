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
import numpy as np
import random
import string
from decimal import Decimal
from datetime import datetime, timedelta


def generate_random_data(data_type, num_rows):
    if pa.types.is_int8(data_type):
        return pa.array(np.random.randint(-128, 127, num_rows, dtype=np.int8))
    elif pa.types.is_int16(data_type):
        return pa.array(np.random.randint(-32768, 32767, num_rows, dtype=np.int16))
    elif pa.types.is_int32(data_type):
        return pa.array(
            np.random.randint(-2147483648, 2147483647, num_rows, dtype=np.int32)
        )
    elif pa.types.is_int64(data_type):
        return pa.array(
            np.random.randint(
                -9223372036854775808, 9223372036854775807, num_rows, dtype=np.int64
            )
        )
    elif pa.types.is_uint8(data_type):
        return pa.array(np.random.randint(0, 255, num_rows, dtype=np.uint8))
    elif pa.types.is_uint16(data_type):
        return pa.array(np.random.randint(0, 65535, num_rows, dtype=np.uint16))
    elif pa.types.is_uint32(data_type):
        return pa.array(np.random.randint(0, 4294967295, num_rows, dtype=np.uint32))
    elif pa.types.is_uint64(data_type):
        return pa.array(
            np.random.randint(0, 18446744073709551615, num_rows, dtype=np.uint64)
        )
    elif pa.types.is_float32(data_type):
        return pa.array(np.random.rand(num_rows).astype(np.float32))
    elif pa.types.is_float64(data_type):
        return pa.array(np.random.rand(num_rows).astype(np.float64))
    elif pa.types.is_string(data_type):
        charset = string.ascii_lowercase + string.ascii_uppercase + string.digits
        return pa.array(
            ["".join(random.choices(charset, k=8)) for _ in range(num_rows)]
        )
    elif pa.types.is_binary(data_type):
        return pa.array([random.randbytes(8) for _ in range(num_rows)])
    elif pa.types.is_boolean(data_type):
        return pa.array(np.random.choice([True, False], num_rows))
    elif pa.types.is_date32(data_type):
        base_date = datetime(1970, 1, 1)
        return pa.array(
            [
                (base_date + timedelta(days=random.randint(0, 10000))).date()
                for _ in range(num_rows)
            ],
            type=pa.date32(),
        )
    elif pa.types.is_date64(data_type):
        base_date = datetime(1970, 1, 1)
        return pa.array(
            [
                (
                    base_date
                    + timedelta(
                        milliseconds=random.randint(0, 10000 * 24 * 60 * 60 * 1000)
                    )
                ).date()
                for _ in range(num_rows)
            ],
            type=pa.date64(),
        )
    elif pa.types.is_timestamp(data_type):
        return pa.array(
            [
                datetime.utcnow() + timedelta(seconds=random.randint(0, 10000))
                for _ in range(num_rows)
            ],
            type=pa.timestamp("ns"),
        )
    elif pa.types.is_decimal(data_type):
        return pa.array(
            [
                Decimal(
                    f"{random.randint(10**7, 10**8-1)}.{random.randint(0, 10**2-1)}"
                )
                for _ in range(num_rows)
            ],
            type=pa.decimal128(10, 2),
        )
    elif pa.types.is_list(data_type):
        return pa.array(
            [[random.randint(0, 100) for _ in range(3)] for _ in range(num_rows)],
            type=pa.list_(pa.int32()),
        )
    elif pa.types.is_struct(data_type):
        struct_type = pa.struct([("field1", pa.int32()), ("field2", pa.float64())])
        return pa.array(
            [
                {"field1": random.randint(0, 100), "field2": random.random()}
                for _ in range(num_rows)
            ],
            type=struct_type,
        )
    elif pa.types.is_dictionary(data_type):
        return pa.array(
            [f"key_{i}" for i in range(num_rows)],
            type=pa.dictionary(pa.int32(), pa.string()),
        )
    else:
        return pa.nulls(num_rows, type=data_type)


data_types = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.float32(),
    pa.float64(),
    pa.string(),
    pa.binary(),
    pa.bool_(),
    pa.date32(),
    pa.date64(),
    pa.timestamp("ns"),
    pa.decimal128(10, 2),
    pa.list_(pa.int32()),
    pa.struct([("field1", pa.int32()), ("field2", pa.float64())]),
    pa.dictionary(pa.int32(), pa.string()),
    pa.null(),
]

schema = pa.schema([(f"col_{j}", data_type) for j, data_type in enumerate(data_types)])

num_rows_per_batch = 1000
num_batches = 100

path = "random.arrows"

with pa.ipc.new_stream(path, schema) as writer:
    for i in range(0, num_batches):
        columns = {
            f"col_{j}": generate_random_data(data_type, num_rows_per_batch)
            for j, data_type in enumerate(data_types)
        }
        writer.write_batch(pa.RecordBatch.from_pydict(columns))

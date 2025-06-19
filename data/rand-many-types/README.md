<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# rand-many-types

This directory contains a file `random.arrows` in Arrow IPC stream format with randomly generated values in 20+ columns exercising many different Arrow data types. The Python script `generate.py` that generated the data file is included.

The same data is also included as a file in Arrow IPC file format (`random.arrow`), as a Parquet file (`random.parquet`), and as a DuckDB database file (`random.duckdb`) as the table named `random`. The Python and SQL used to generate these files is included.

To re-generate the data files (for example, if you change `generate.py`),

1. Make sure `duckdb` is in your path and activate a Python environment with the packages in `./requirements.txt`
2. Run `make`

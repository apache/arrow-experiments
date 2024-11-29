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

# HTTP GET Arrow Data: Indirect Python Client Example with `requests`

This directory contains an example of an HTTP client implemented in Python using the built-in [`urllib.request`](https://docs.python.org/3/library/urllib.request.html) module. The client:
1. Sends a GET request to the server to get a JSON listing of the filenames of available `.arrows` files.
2. Sends GET requests to download each of the `.arrows` files from the server.
3. Loads the contents of each file into an in-memory PyArrow Table.

To run this example, first start one of the indirect server examples in the parent directory, then:

```sh
pip install requests pyarrow
python client.py
```

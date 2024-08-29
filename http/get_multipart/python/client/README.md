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

# HTTP GET Arrow Data in multipart/mixed: Python Client Example

This directory contains an example of a Python HTTP client that receives a
`multipart/mixed` response from the server. The client:
1. Sends an HTTP GET request to a server.
2. Receives an HTTP 200 response from the server, with the response body
   containing a `multipart/mixed` response.
3. Parses the `multipart/mixed` response using the `email` module.[^1]
4. Extracts the JSON part, parses it and prints a preview of the JSON data.
5. Extracts the Arrow stream part, reads the Arrow stream, and sums the
   total number of records in the entire Arrow stream.
6. Extracts the plain text part and prints it as it is.

To run this example, first start one of the server examples in the parent
directory, then:

```sh
pip install pyarrow
python simple_client.py
```

> [!WARNING]
> This `simple_client.py` parses the multipart response using the multipart
> message parser from the Python `email` module. This module puts the entire
> message in memory and seems to spend a lot of time looking for part delimiter
> and encoding/decoding the parts.
>
> The overhead of `multipart/mixed` parsing is 85% on my machine and after the
> ~1GB Arrow Stream message is fully in memory, it takes only 0.06% of the total
> execution time to parse it.

[^1]: The `multipart/mixed` standard, used by HTTP, is derived from the MIME
standard used in email.

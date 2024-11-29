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

# HTTP GET Arrow Data: Range Request Node.js Server Example

The example in this directory shows how to use the Node.js package [`serve`](https://www.npmjs.com/package/serve) (which supports range requests) to serve a static Arrow IPC stream file over HTTP.

To run this example, copy the file `random.arrows` from the directory `data/rand-many-types/` into the current directory:

```sh
cp ../../../../data/rand-many-types/random.arrows .
```

Then start the HTTP server to serve this file:

```sh
npx --yes serve -l 8008
```

> [!NOTE]  
> The npm package `serve` _should_ automatically set the `Content-Type` header to `application/vnd.apache.arrow.stream` when serving a file with extension `.arrows`, because [the Arrow IPC stream format is officially registered with IANA](https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.stream) and most web servers including `serve` use registration data from IANA to determine the media type of a file based on its file extension and set the `Content-Type` header to that media type when serving a file with that extension. However, this is not working with `.arrows` files in the `serve` package, seemingly because of a problem with the npm package [`mimedb`](https://github.com/jshttp/mime-db) which `serve` depends on. So the file `serve.json` is used to set the `Content-Type` header correctly when serving `.arrows` files.

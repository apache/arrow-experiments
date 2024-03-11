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

library(httr)
library(tictoc)
suppressPackageStartupMessages(library(arrow))

url <- 'http://localhost:8008'

tic()

response <- GET(url)
buffer <- content(response, "raw")
reader <- RecordBatchStreamReader$create(buffer)
table <- reader$read_table()

# or:
#batches <- reader$batches()
# but this is very slow (https://github.com/apache/arrow/issues/39090)


# or:
#result <- read_ipc_stream(buffer, as_data_frame = FALSE)

# or:
#result <- read_ipc_stream(url, as_data_frame = FALSE)

toc()

cat(format(table$nbytes(), scientific = FALSE), "bytes received\n")
cat(table$num_rows, "records received\n")

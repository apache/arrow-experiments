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

library(gert)
library(arrow, warn.conflicts = FALSE)

# Assumes the working directory is the root arrow-experiments
out_stream <- file.path(getwd(), "data/arrow-commits/arrow-commits.arrows")
out_jsonl <- file.path(getwd(), "data/arrow-commits/arrow-commits.jsonl")

# ...and an apache/arrow checkout at ../arrow
commits <- withr::with_dir("../arrow", {
  gert::git_log(max = .Machine$integer.max)
})

# Best if names and email addresses are not included in example data
commits$author <- NULL

# Ensure times are UTC
commits$time <- lubridate::with_tz(commits$time, "UTC")

# Just take the first line of each commit message
commits$message <- vapply(
  commits$message,
  function(x) strsplit(x, "\n+")[[1]][1],
  character(1),
  USE.NAMES = FALSE
)

# Don't include any R metadata in the example Arrow file
commits_table <- arrow_table(commits)
commits_table$metadata <- NULL

# R bindings don't expose non-default batch size, so do the chunking manually
batch_size <- 1024
num_batches <- nrow(commits_table) %/% batch_size + 1
batch <- function(i) {
  begin <- (i - 1) * batch_size + 1
  end <- min(begin + batch_size - 1, nrow(commits_table))
  commits_table[begin:end, ]
}

fs <- LocalFileSystem$create()
out <- fs$OpenOutputStream(out_stream)
writer <- RecordBatchStreamWriter$create(out, commits_table$schema)
for (i in seq_len(num_batches)) {
  writer$write_table(batch(i))
}
writer$close()

# Check a simple read of the Arrow stream
stopifnot(identical(read_ipc_stream(out_stream), commits))

# Also write a .jsonl version
withr::with_connection(list(con = file(out_jsonl)), {
  open(con, "w")
  for (i in seq_len(nrow(commits))) {
    item <- as.list(commits[i, , drop = FALSE])
    line <- jsonlite::toJSON(item, POSIXt = "ISO8601", auto_unbox = TRUE)
    writeLines(line, con)
  }
})

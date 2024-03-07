
library(gert)
library(arrow, warn.conflicts = FALSE)

# Assumes the working directory is
out_stream <- file.path(getwd(), "data/arrow-commits/arrow-commits.arrows")

# ...and that an apache/arrow checkout at ../arrow
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

# Don't include any R metadata in the example file
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

# Check a simple read of the file
stopifnot(identical(read_ipc_stream(out_stream), commits))

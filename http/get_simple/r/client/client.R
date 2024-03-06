library(httr)
library(tictoc)
suppressPackageStartupMessages(library(arrow))

url <- 'http://localhost:8000'

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

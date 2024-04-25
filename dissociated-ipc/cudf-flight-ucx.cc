// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/status.h>
#include <arrow/util/logging.h>
#include <arrow/util/uri.h>
#include <gflags/gflags.h>

#include "cudf-flight-ucx.h"

DEFINE_int32(port, 31337, "port to listen or connect");
DEFINE_string(address, "127.0.0.1", "address to connect to");
DEFINE_bool(client, false, "run the client");

int main(int argc, char** argv) {
  arrow::util::ArrowLog::StartArrowLog("cudf-flight-poc",
                                       arrow::util::ArrowLogLevel::ARROW_DEBUG);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_client) {
    ARROW_CHECK_OK(run_client(FLAGS_address, FLAGS_port));
  } else {
    ARROW_CHECK_OK(run_server(FLAGS_address, FLAGS_port));
  }
}

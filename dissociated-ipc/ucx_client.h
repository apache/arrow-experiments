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

#pragma once

#include "ucx_conn.h"
#include "ucx_utils.h"

#include <memory>
#include <string>

#include <arrow/status.h>

class UcxClient {
 public:
  UcxClient() = default;
  ~UcxClient() = default;

  arrow::Status Init(const std::string& host, const int32_t port);
  arrow::Result<std::unique_ptr<utils::Connection>> CreateConn();

 private:
  std::shared_ptr<utils::UcpContext> ucp_context_;
  struct sockaddr_storage connect_addr_;
  size_t addrlen_;
};

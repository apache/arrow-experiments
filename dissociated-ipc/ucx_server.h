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

#include <atomic>
#include <memory>
#include <queue>
#include <thread>

#include "ucx_conn.h"
#include "ucx_utils.h"

#include "arrow/flight/types.h"
#include "arrow/gpu/cuda_context.h"
#include "arrow/status.h"

class UcxServer {
 public:
  virtual ~UcxServer() = default;
  arrow::Status Init(const std::string& host, const int32_t port);

  arrow::Status Wait();  
  virtual arrow::Status Shutdown();

  inline void set_cuda_context(std::shared_ptr<arrow::cuda::CudaContext> ctx) {
    cuda_context_ = std::move(ctx);
  }

 protected:
  inline arrow::flight::Location location() const { return location_; }

  struct ClientWorker {
    std::shared_ptr<utils::UcpWorker> worker_;
    std::unique_ptr<utils::Connection> conn_;
  };

  virtual arrow::Status setup_handlers(ClientWorker* worker) = 0;
  virtual arrow::Status do_work(ClientWorker* worker) = 0;

 private:
  static void HandleIncomingConnection(ucp_conn_request_h connection_request,
                                       void* data) {
    UcxServer* server = reinterpret_cast<UcxServer*>(data);
    server->EnqueueClient(connection_request);
  }

  void DriveConnections();
  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::lock_guard<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
  }

  void HandleConnection(ucp_conn_request_h request);
  arrow::Result<std::shared_ptr<ClientWorker>> CreateWorker();

 protected:
  std::atomic<size_t> counter_{0};
  arrow::flight::Location location_;
  std::shared_ptr<utils::UcpContext> ucp_context_;
  std::shared_ptr<utils::UcpWorker> worker_conn_;
  ucp_listener_h listener_;

  std::atomic<bool> listening_;
  std::thread listener_thread_;
  // std::thread::join cannot be called concurrently
  std::mutex join_mutex_;
  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;

  std::shared_ptr<arrow::cuda::CudaContext> cuda_context_;
};
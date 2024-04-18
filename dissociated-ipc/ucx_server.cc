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

#include <arpa/inet.h>
#include <netdb.h>

#include "ucx_server.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string.h"

namespace {
arrow::Result<std::shared_ptr<utils::UcpContext>> init_ucx(
    struct sockaddr_storage connect_addr) {
  ucp_config_t* ucp_config;
  ucp_params_t ucp_params;
  ucs_status_t status = ucp_config_read(nullptr, nullptr, &ucp_config);
  RETURN_NOT_OK(utils::FromUcsStatus("ucp_config_read", status));

  // if location is ipv6, adjust config
  if (connect_addr.ss_family == AF_INET6) {
    status = ucp_config_modify(ucp_config, "AF_PRIO", "inet6");
    RETURN_NOT_OK(utils::FromUcsStatus("ucp_config_modify", status));
  }

  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask =
      UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
  ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_TAG | UCP_FEATURE_RMA |
                        UCP_FEATURE_WAKEUP | UCP_FEATURE_STREAM;
  ucp_params.mt_workers_shared = UCS_THREAD_MODE_MULTI;
  ucp_params.name = "cuda-flight-ucx";

  ucp_context_h ucp_context;
  status = ucp_init(&ucp_params, ucp_config, &ucp_context);
  ucp_config_release(ucp_config);
  RETURN_NOT_OK(utils::FromUcsStatus("ucp_init", status));
  return std::make_shared<utils::UcpContext>(ucp_context);
}

arrow::Result<std::shared_ptr<utils::UcpWorker>> create_listener_worker(
    std::shared_ptr<utils::UcpContext> ctx) {
  ucp_worker_params_t worker_params;
  ucs_status_t status;

  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

  ucp_worker_h worker;
  status = ucp_worker_create(ctx->get(), &worker_params, &worker);
  RETURN_NOT_OK(utils::FromUcsStatus("ucp_worker_create", status));
  return std::make_shared<utils::UcpWorker>(std::move(ctx), worker);
}
}  // namespace

arrow::Status UcxServer::Init(const std::string& host, const int32_t port) {
  struct sockaddr_storage listen_addr;
  ARROW_ASSIGN_OR_RAISE(auto addrlen, utils::to_sockaddr(host, port, &listen_addr));

  ARROW_ASSIGN_OR_RAISE(ucp_context_, init_ucx(listen_addr));
  ARROW_ASSIGN_OR_RAISE(worker_conn_, create_listener_worker(ucp_context_));

  {
    ucp_listener_params_t params;
    ucs_status_t status;

    params.field_mask =
        UCP_LISTENER_PARAM_FIELD_SOCK_ADDR | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
    params.sockaddr.addrlen = addrlen;
    params.conn_handler.cb = HandleIncomingConnection;
    params.conn_handler.arg = this;

    status = ucp_listener_create(worker_conn_->get(), &params, &listener_);
    RETURN_NOT_OK(utils::FromUcsStatus("ucp_listener_create", status));

    // get real address/port
    ucp_listener_attr_t attr;
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    status = ucp_listener_query(listener_, &attr);
    RETURN_NOT_OK(utils::FromUcsStatus("ucp_listener_query", status));

    std::string raw_uri = "ucx://";
    if (host.find(":") != std::string::npos) {
      raw_uri += '[';
      raw_uri += host;
      raw_uri += ']';
    } else {
      raw_uri += host;
    }

    using arrow::internal::ToChars;

    raw_uri += ":";
    raw_uri +=
        ToChars(ntohs(reinterpret_cast<const sockaddr_in*>(&attr.sockaddr)->sin_port));

    ARROW_ASSIGN_OR_RAISE(location_, arrow::flight::Location::Parse(raw_uri));
  }

  {
    listening_.store(true);
    std::thread listener_thread(&UcxServer::DriveConnections, this);
    listener_thread_.swap(listener_thread);
  }

  return arrow::Status::OK();
}

arrow::Status UcxServer::Wait() {
  std::lock_guard<std::mutex> guard(join_mutex_);
  try {
    listener_thread_.join();
  } catch (const std::system_error& e) {
    if (e.code() != std::errc::invalid_argument) {
      return arrow::Status::UnknownError("could not Wait(): ", e.what());
    }
    // else server wasn't running anyways
  }
  return arrow::Status::OK();
}

arrow::Status UcxServer::Shutdown() {
  if (!listening_.load()) return arrow::Status::OK();

  arrow::Status status;
  // wait for current running things to finish
  listening_.store(false);
  RETURN_NOT_OK(
      utils::FromUcsStatus("ucp_worker_signal", ucp_worker_signal(worker_conn_->get())));
  status &= Wait();

  {
    // reject all pending connections
    std::lock_guard<std::mutex> guard(pending_connections_mutex_);
    while (!pending_connections_.empty()) {
      status &= utils::FromUcsStatus(
          "ucp_listener_reject",
          ucp_listener_reject(listener_, pending_connections_.front()));
      pending_connections_.pop();
    }
    ucp_listener_destroy(listener_);
    worker_conn_.reset();
  }

  ucp_context_.reset();
  return status;
}

void UcxServer::DriveConnections() {
  while (listening_.load()) {
    // wait for server to recieve connection request from client
    while (ucp_worker_progress(worker_conn_->get())) {
    }
    {
      // check for requests in queue
      std::lock_guard<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        ucp_conn_request_h request = pending_connections_.front();
        pending_connections_.pop();

        std::thread(&UcxServer::HandleConnection, this, request).detach();
      }
    }

    // check listening_ in case we're shutting down.
    // it's possible that shutdown was called while we were in
    // ucp_worker_progress above, in which case if we don't check
    // listening_ here, we'll enter ucp_worker_wait and get stuck.
    if (!listening_.load()) break;
    auto status = ucp_worker_wait(worker_conn_->get());
    if (status != UCS_OK) {
      ARROW_LOG(WARNING) << utils::FromUcsStatus("ucp_worker_wait", status).ToString();
    }
  }
}

void UcxServer::HandleConnection(ucp_conn_request_h request) {
  using arrow::internal::ToChars;
  std::string peer = "unknown:" + ToChars(counter_++);
  {
    ucp_conn_request_attr_t request_attr;
    std::memset(&request_attr, 0, sizeof(request_attr));
    request_attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    if (ucp_conn_request_query(request, &request_attr) == UCS_OK) {
      ARROW_UNUSED(utils::SockaddrToString(request_attr.client_address).Value(&peer));
    }
  }
  ARROW_LOG(DEBUG) << peer << ": Received connection request";

  auto maybe_worker = CreateWorker();
  if (!maybe_worker.ok()) {
    ARROW_LOG(ERROR) << peer << ": failed to create worker"
                     << maybe_worker.status().ToString();
    auto status = ucp_listener_reject(listener_, request);
    if (status != UCS_OK) {
      ARROW_LOG(ERROR) << peer << ": "
                       << utils::FromUcsStatus("ucp_listener_reject", status).ToString();
    }
    return;
  }

  auto worker = maybe_worker.MoveValueUnsafe();
  worker->conn_ = std::make_unique<utils::Connection>(worker->worker_);
  auto status = worker->conn_->CreateEndpoint(request);
  if (!status.ok()) {
    ARROW_LOG(ERROR) << peer << ": failed to create endpoint and connection: "
                     << status.ToString();
    return;
  }

  if (cuda_context_) {
    auto result = cuCtxPushCurrent(reinterpret_cast<CUcontext>(cuda_context_->handle()));
    if (result != CUDA_SUCCESS) {
      const char* err_name = "\0";
      const char* err_string = "\0";
      cuGetErrorName(result, &err_name);
      cuGetErrorString(result, &err_string);
      ARROW_LOG(ERROR) << peer << ": failed pushing cuda context on thread: " << err_name
                       << " - " << err_string;
      return;
    }
  }

  auto st = do_work(worker.get());
  if (!st.ok()) {
    ARROW_LOG(ERROR) << peer << ": error from do_work: " << st.ToString();
  }

  while (!worker->conn_->is_closed()) {
    worker->conn_->Progress();
  }

  // clean up
  status = worker->conn_->Close();
  if (!status.ok()) {
    ARROW_LOG(ERROR) << peer
                     << ": failed to close worker connection: " << status.ToString();
  }
  worker->worker_.reset();
  worker->conn_.reset();
  ARROW_LOG(DEBUG) << peer << ": disconnected";
}

arrow::Result<std::shared_ptr<UcxServer::ClientWorker>> UcxServer::CreateWorker() {
  auto worker = std::make_shared<ClientWorker>();

  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask =
      UCP_WORKER_PARAM_FIELD_THREAD_MODE | UCP_WORKER_PARAM_FIELD_FLAGS;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;
  worker_params.flags = UCP_WORKER_FLAG_IGNORE_REQUEST_LEAK;

  ucp_worker_h ucp_worker;
  ARROW_RETURN_NOT_OK(utils::FromUcsStatus(
      "ucp_worker_create",
      ucp_worker_create(ucp_context_->get(), &worker_params, &ucp_worker)));

  worker->worker_ = std::make_shared<utils::UcpWorker>(ucp_context_, ucp_worker);
  ARROW_RETURN_NOT_OK(setup_handlers(worker.get()));
  return worker;
}
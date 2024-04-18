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

#include "ucx_client.h"
#include "ucx_utils.h"

arrow::Status UcxClient::Init(const std::string& host, const int32_t port) {
  ucp_config_t* ucp_config;
  ucp_params_t ucp_params;
  ucs_status_t status;

  status = ucp_config_read(nullptr, nullptr, &ucp_config);
  ARROW_RETURN_NOT_OK(utils::FromUcsStatus("ucp_config_read", status));

  // if location is IPv6 must adjust UCX config
  // we assume locations always resolve to IPv6 or IPv4
  // but that's not necessarily true
  ARROW_ASSIGN_OR_RAISE(addrlen_, utils::to_sockaddr(host, port, &connect_addr_));
  if (connect_addr_.ss_family == AF_INET6) {
    ARROW_RETURN_NOT_OK(utils::FromUcsStatus(
        "ucp_config_modify", ucp_config_modify(ucp_config, "AF_PRIO", "inet6")));
  }

  std::memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
  ucp_params.features = UCP_FEATURE_WAKEUP | UCP_FEATURE_AM | UCP_FEATURE_RMA |
                        UCP_FEATURE_STREAM | UCP_FEATURE_TAG;

  ucp_context_h ucp_context;
  status = ucp_init(&ucp_params, ucp_config, &ucp_context);
  ucp_config_release(ucp_config);

  ARROW_RETURN_NOT_OK(utils::FromUcsStatus("ucp_init", status));
  ucp_context_.reset(new utils::UcpContext(ucp_context));
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<utils::Connection>> UcxClient::CreateConn() {
  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask =
      UCP_WORKER_PARAM_FIELD_THREAD_MODE | UCP_WORKER_PARAM_FIELD_FLAGS;
  worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;
  worker_params.flags = UCP_WORKER_FLAG_IGNORE_REQUEST_LEAK;

  ucp_worker_h ucp_worker;
  ucs_status_t status =
      ucp_worker_create(ucp_context_->get(), &worker_params, &ucp_worker);
  ARROW_RETURN_NOT_OK(utils::FromUcsStatus("ucp_worker_create", status));

  auto cnxn = std::make_unique<utils::Connection>(
      std::make_shared<utils::UcpWorker>(ucp_context_, ucp_worker));
  ARROW_RETURN_NOT_OK(cnxn->CreateEndpoint(connect_addr_, addrlen_));

  return cnxn;
}

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

#include <ucp/api/ucp.h>
#include <future>
#include <memory>

#include "arrow/util/logging.h"
#include "ucx_utils.h"

namespace utils {
class Connection {
 public:
  Connection(std::shared_ptr<UcpWorker> worker);
  Connection(std::shared_ptr<UcpWorker> worker, ucp_ep_h endpoint);

  ARROW_DISALLOW_COPY_AND_ASSIGN(Connection);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(Connection);
  ~Connection() { DCHECK(!ucp_worker_) << "Connection was not closed!"; }

  arrow::Status CreateEndpoint(ucp_conn_request_h request);
  arrow::Status CreateEndpoint(const sockaddr_storage& addr, const size_t addrlen);
  arrow::Status Flush();
  arrow::Status Close();
  inline bool is_closed() const { return closed_; }
  inline unsigned int Progress() { return ucp_worker_progress(ucp_worker_->get()); }
  inline ucs_status_t WorkerWait() { return ucp_worker_wait(ucp_worker_->get()); }

  arrow::Status SetAMHandler(unsigned int id, void* user_data, ucp_am_recv_callback_t cb);

  arrow::Result<std::pair<ucp_tag_recv_info_t, ucp_tag_message_h>> ProbeForTag(
      ucp_tag_t tag, ucp_tag_t mask, int remove);
  arrow::Result<std::pair<ucp_tag_recv_info_t, ucp_tag_message_h>> ProbeForTagSync(
      ucp_tag_t tag, ucp_tag_t mask, int remove);
  arrow::Status RecvTagData(ucp_tag_message_h msg, void* buffer, const size_t count,
                            void* user_data, ucp_tag_recv_nbx_callback_t cb,
                            const ucs_memory_type_t memory_type);
  ucs_status_t RecvAM(std::promise<std::unique_ptr<arrow::Buffer>> p, const void* header,
                      const size_t header_length, void* data, const size_t data_length,
                      const ucp_am_recv_param_t* param);

  arrow::Status SendAM(unsigned int id, const void* data, const int64_t size);
  arrow::Status SendAMIov(unsigned int id, const ucp_dt_iov_t* iov, const size_t iov_cnt,
                          void* user_data, ucp_send_nbx_callback_t cb,
                          const ucs_memory_type_t memory_type);
  arrow::Status SendTagIov(ucp_tag_t tag, const ucp_dt_iov_t* iov, const size_t iov_cnt,
                           void* user_data, ucp_send_nbx_callback_t cb,
                           const ucs_memory_type_t memory_type);
  arrow::Status SendTagSync(ucp_tag_t tag, const void* buffer, const size_t count);

 protected:
  static void err_cb(void* arg, ucp_ep_h ep, ucs_status_t status) {
    if (!is_ignorable_disconnect_error(status)) {
      ARROW_LOG(DEBUG) << FromUcsStatus("error handling callback", status).ToString();
    }
    Connection* cnxn = reinterpret_cast<Connection*>(arg);
    cnxn->closed_ = true;
  }

  inline arrow::Status CheckClosed() {
    if (!remote_endpoint_) {
      return arrow::Status::Invalid("connection is closed");
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<utils::UcpWorker> ucp_worker_;
  ucp_ep_h remote_endpoint_;

  bool closed_ { false };
};
}  // namespace utils
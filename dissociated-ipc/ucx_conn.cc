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

#include "ucx_conn.h"

#include "arrow/device.h"

namespace utils {

ucs_status_t wait_for_request(ucs_status_ptr_t request, UcpWorker& worker) {
  ucs_status_t status = UCS_OK;
  if (UCS_PTR_IS_ERR(request)) {
    status = UCS_PTR_STATUS(request);    
  } else if (UCS_PTR_IS_PTR(request)) {
    while ((status = ucp_request_check_status(request)) == UCS_INPROGRESS) {
      ucp_worker_progress(worker.get());
    }
    ucp_request_free(request);
  } else {
    DCHECK(!request);
  }
  return status;
}

Connection::Connection(std::shared_ptr<UcpWorker> worker)
    : ucp_worker_{std::move(worker)} {}

Connection::Connection(std::shared_ptr<UcpWorker> worker, ucp_ep_h endpoint)
    : ucp_worker_{std::move(worker)}, remote_endpoint_(endpoint) {}

arrow::Status Connection::CreateEndpoint(ucp_conn_request_h request) {
  ucs_status_t status;
  ucp_ep_params_t params;
  std::memset(&params, 0, sizeof(params));
  params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST | UCP_EP_PARAM_FIELD_ERR_HANDLER;
  params.err_handler.arg = this;
  params.err_handler.cb = Connection::err_cb;
  params.conn_request = request;

  return FromUcsStatus("ucp_ep_create",
                       ucp_ep_create(ucp_worker_->get(), &params, &remote_endpoint_));
}

arrow::Status Connection::CreateEndpoint(const sockaddr_storage& connect_addr,
                                         const size_t addrlen) {
  std::string peer;
  ARROW_UNUSED(SockaddrToString(connect_addr).Value(&peer));
  ARROW_LOG(DEBUG) << "Connecting to " << peer;

  ucp_ep_params_t params;
  params.field_mask =
      UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_NAME | UCP_EP_PARAM_FIELD_SOCK_ADDR;
  params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER | UCP_EP_PARAMS_FLAGS_SEND_CLIENT_ID;
  params.name = "UcxConn";
  params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&connect_addr);
  params.sockaddr.addrlen = addrlen;

  return FromUcsStatus("ucp_ep_create",
                       ucp_ep_create(ucp_worker_->get(), &params, &remote_endpoint_));
}

arrow::Status Connection::Flush() {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t param;
  param.op_attr_mask = 0;
  void* request = ucp_ep_flush_nbx(remote_endpoint_, &param);
  if (!request) {
    return arrow::Status::OK();
  }

  return utils::FromUcsStatus("ucp_ep_flush_nbx",
                              wait_for_request(request, *ucp_worker_));
}

arrow::Status Connection::Close() {
  ucp_request_param_t params;
  std::memset(&params, 0, sizeof(ucp_request_param_t));
  params.flags = UCP_EP_CLOSE_FLAG_FORCE;

  void* request = ucp_ep_close_nbx(remote_endpoint_, &params);
  auto status = wait_for_request(request, *ucp_worker_);

  remote_endpoint_ = nullptr;
  ucp_worker_.reset();
  if (status != UCS_OK && !is_ignorable_disconnect_error(status)) {
    return FromUcsStatus("close conn", status);
  }
  return arrow::Status::OK();
}

arrow::Status Connection::SetAMHandler(unsigned int id, void* user_data,
                                       ucp_am_recv_callback_t cb) {
  ucp_am_handler_param_t params;
  params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID;
  params.id = id;
  if (user_data) {
    params.field_mask |= UCP_AM_HANDLER_PARAM_FIELD_ARG;
    params.arg = user_data;
  }
  if (cb) {
    params.field_mask |= UCP_AM_HANDLER_PARAM_FIELD_CB;
    params.cb = cb;
  }
  return utils::FromUcsStatus(
      "ucp_worker_set_am_recv_handler",
      ucp_worker_set_am_recv_handler(ucp_worker_->get(), &params));
}

arrow::Result<std::pair<ucp_tag_recv_info_t, ucp_tag_message_h>> Connection::ProbeForTag(
    ucp_tag_t tag, ucp_tag_t mask, int remove) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_tag_recv_info_t info_tag;
  auto msg_tag = ucp_tag_probe_nb(ucp_worker_->get(), tag, mask, remove, &info_tag);
  return std::make_pair(info_tag, msg_tag);
}

arrow::Result<std::pair<ucp_tag_recv_info_t, ucp_tag_message_h>>
Connection::ProbeForTagSync(ucp_tag_t tag, ucp_tag_t mask, int remove) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_tag_recv_info_t info_tag;
  ucp_tag_message_h msg_tag;
  while (true) {
    msg_tag = ucp_tag_probe_nb(ucp_worker_->get(), tag, mask, remove, &info_tag);
    if (msg_tag != nullptr) {
      // success
      break;
    } else if (ucp_worker_progress(ucp_worker_->get())) {
      // some events polled, try again
      continue;
    }

    // ucp_worker_progress 0, so we sleep
    // following blocked method used to poll internal file descriptor
    // to make CPU idle and not spin loop
    ARROW_RETURN_NOT_OK(
        FromUcsStatus("ucp_worker_wait", ucp_worker_wait(ucp_worker_->get())));
  }

  return std::make_pair(info_tag, msg_tag);
}

struct RndvPromiseBuffer {
  std::promise<std::unique_ptr<arrow::Buffer>> p;
  std::unique_ptr<arrow::Buffer> buf;
};

ucs_status_t Connection::RecvAM(std::promise<std::unique_ptr<arrow::Buffer>> p,
                                const void* header, const size_t header_length,
                                void* data, const size_t data_length,
                                const ucp_am_recv_param_t* param) {
  if (data_length > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    ARROW_LOG(ERROR) << "cannot allocate buffer greater than 2 GiB, requested: "
                     << data_length;
    return UCS_ERR_IO_ERROR;
  }

  if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA) {
    // data provided can be held by us. return UCS_INPROGRESS to make the data persist
    // and we will eventually use ucp_am_data_release to release it.
    auto buffer = std::make_unique<UcxDataBuffer>(ucp_worker_, data, data_length);
    p.set_value(std::move(buffer));
    return UCS_INPROGRESS;
  }

  // rendezvous protocol
  if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
    auto maybe_buffer = arrow::default_cpu_memory_manager()->AllocateBuffer(data_length);
    if (!maybe_buffer.ok()) {
      ARROW_LOG(ERROR) << "could not allocate buffer for message: "
                       << maybe_buffer.status().ToString();
      return UCS_ERR_NO_MEMORY;
    }

    auto buffer = maybe_buffer.MoveValueUnsafe();
    void* dest = reinterpret_cast<void*>(buffer->mutable_address());

    ucp_request_param_t recv_param;
    recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                              UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
    recv_param.memory_type = UCS_MEMORY_TYPE_HOST;
    recv_param.user_data = new RndvPromiseBuffer{std::move(p), std::move(buffer)};
    recv_param.cb.recv_am = [](void* request, ucs_status_t status, size_t length,
                               void* user_data) {
      auto p = std::unique_ptr<RndvPromiseBuffer>(
          reinterpret_cast<RndvPromiseBuffer*>(user_data));
      if (request) {
        ucp_request_free(request);
      }
      if (status == UCS_OK) {
        p->p.set_value(std::move(p->buf));
      } else {
        ARROW_LOG(ERROR) << FromUcsStatus("ucp_am_recv_data_nbx cb", status).ToString();
        p->p.set_value(nullptr);
      }
    };
    void* request =
        ucp_am_recv_data_nbx(ucp_worker_->get(), data, dest, data_length, &recv_param);
    if (UCS_PTR_IS_ERR(request)) {
      return UCS_PTR_STATUS(request);
    }
    return UCS_OK;
  }

  auto maybe_buffer = arrow::default_cpu_memory_manager()->AllocateBuffer(data_length);
  if (!maybe_buffer.ok()) {
    ARROW_LOG(ERROR) << "could not allocate buffer for message: "
                     << maybe_buffer.status().ToString();
    return UCS_ERR_NO_MEMORY;
  }
  auto buffer = maybe_buffer.MoveValueUnsafe();
  std::memcpy(buffer->mutable_data(), data, data_length);
  p.set_value(std::move(buffer));
  return UCS_OK;
}

arrow::Status Connection::RecvTagData(ucp_tag_message_h msg, void* buffer,
                                      const size_t count, void* user_data,
                                      ucp_tag_recv_nbx_callback_t cb,
                                      const ucs_memory_type_t memory_type) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t recv_param;
  recv_param.op_attr_mask = UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
                            UCP_OP_ATTR_FIELD_DATATYPE |
                            UCP_OP_ATTR_FIELD_MEMORY_TYPE;
  recv_param.datatype = ucp_dt_make_contig(1);
  recv_param.memory_type = memory_type;
  if (user_data) {
    recv_param.user_data = user_data;
    recv_param.op_attr_mask |= UCP_OP_ATTR_FIELD_USER_DATA;
  }
  if (cb) {
    recv_param.cb.recv = cb;
    recv_param.op_attr_mask |= UCP_OP_ATTR_FIELD_CALLBACK;
  }

  auto request =
      ucp_tag_msg_recv_nbx(ucp_worker_->get(), buffer, count, msg, &recv_param);
  return FromUcsStatus("recvtagdata", wait_for_request(request, *ucp_worker_));
}

arrow::Status Connection::SendAM(unsigned int id, const void* data, const int64_t size) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t request_param;
  request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
  request_param.flags = UCP_AM_SEND_FLAG_REPLY;

  auto request =
      ucp_am_send_nbx(remote_endpoint_, id, nullptr, 0, data, size, &request_param);
  return FromUcsStatus("ucp_am_send_nbx", wait_for_request(request, *ucp_worker_));
}

arrow::Status Connection::SendAMIov(unsigned int id, const ucp_dt_iov_t* iov,
                                    const size_t iov_cnt, void* user_data,
                                    ucp_send_nbx_callback_t cb,
                                    const ucs_memory_type_t memory_type) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t request_param;
  request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_DATATYPE |
                               UCP_OP_ATTR_FIELD_MEMORY_TYPE;
  request_param.flags = UCP_AM_SEND_FLAG_REPLY;
  request_param.datatype = UCP_DATATYPE_IOV;
  if (cb) {
    request_param.cb.send = cb;
    request_param.op_attr_mask |= UCP_OP_ATTR_FIELD_CALLBACK;
  }
  if (user_data) {
    request_param.user_data = user_data;
    request_param.op_attr_mask |= UCP_OP_ATTR_FIELD_USER_DATA;
  }
  request_param.memory_type = memory_type;

  void* request =
      ucp_am_send_nbx(remote_endpoint_, id, nullptr, 0, iov, iov_cnt, &request_param);
  if (!request) {
    // request completed immediately, call the cb manually if it exists
    // since it won't be called automatically
    if (cb) cb(request, UCS_OK, user_data);
  } else if (UCS_PTR_IS_ERR(request)) {
    // same thing, call it manually
    auto status = UCS_PTR_STATUS(request);
    if (cb) cb(request, status, user_data);
    return utils::FromUcsStatus("ucp_am_send_nbx", status);
  }

  // otherwise the callback will be called eventually when it completes
  // we can just return success.
  return arrow::Status::OK();
}

arrow::Status Connection::SendTagIov(ucp_tag_t tag, const ucp_dt_iov_t* iov,
                                     const size_t iov_cnt, void* user_data,
                                     ucp_send_nbx_callback_t cb,
                                     const ucs_memory_type_t memory_type) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t request_param;
  request_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
                               UCP_OP_ATTR_FIELD_MEMORY_TYPE;  
  request_param.datatype = UCP_DATATYPE_IOV;
  if (cb) {
    request_param.cb.send = cb;
    request_param.op_attr_mask |= UCP_OP_ATTR_FIELD_CALLBACK;
  }
  if (user_data) {
    request_param.user_data = user_data;
    request_param.op_attr_mask |= UCP_OP_ATTR_FIELD_USER_DATA;
  }
  request_param.memory_type = memory_type;

  void* request = ucp_tag_send_nbx(remote_endpoint_, iov, iov_cnt, tag, &request_param);
  if (!request) {
    // request completed immediately, call the cb manually if it exists
    // since it won't be called automatically
    if (cb) cb(request, UCS_OK, user_data);
  } else if (UCS_PTR_IS_ERR(request)) {
    // same thing, call it manually
    auto status = UCS_PTR_STATUS(request);
    if (cb) cb(request, status, user_data);
    return utils::FromUcsStatus("ucp_tag_send_nbx", status);
  }

  return arrow::Status::OK();
}

arrow::Status Connection::SendTagSync(ucp_tag_t tag, const void* buffer,
                                      const size_t count) {
  ARROW_RETURN_NOT_OK(CheckClosed());

  ucp_request_param_t request_param;
  ucs_status_ptr_t request =
      ucp_tag_send_sync_nbx(remote_endpoint_, buffer, count, tag, &request_param);
  return FromUcsStatus("ucp_tag_send_sync_nbx", wait_for_request(request, *ucp_worker_));
}
}  // namespace utils
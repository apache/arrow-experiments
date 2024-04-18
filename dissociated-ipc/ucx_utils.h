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

#include <arrow/buffer.h>
#include <arrow/status.h>
#include <arrow/util/endian.h>
#include <arrow/util/logging.h>
#include <arrow/util/ubsan.h>

namespace utils {
static inline void Uint32ToBytesLE(const uint32_t in, uint8_t* out) {
  arrow::util::SafeStore(out, arrow::bit_util::ToLittleEndian(in));
}

static inline uint32_t BytesToUint32LE(const uint8_t* in) {
  return arrow::bit_util::FromLittleEndian(arrow::util::SafeLoadAs<uint32_t>(in));
}

class UcpContext final {
 public:
  UcpContext() = default;
  explicit UcpContext(ucp_context_h context) : ucp_context_(context) {}
  ~UcpContext() {
    if (ucp_context_) ucp_cleanup(ucp_context_);
    ucp_context_ = nullptr;
  }

  ucp_context_h get() const {
    DCHECK(ucp_context_);
    return ucp_context_;
  }

 private:
  ucp_context_h ucp_context_{nullptr};
};

class UcpWorker final {
 public:
  UcpWorker() = default;
  UcpWorker(std::shared_ptr<UcpContext> context, ucp_worker_h worker)
      : ucp_context_(std::move(context)), ucp_worker_(worker) {}
  ~UcpWorker() {
    if (ucp_worker_) ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = nullptr;
  }

  ucp_worker_h get() const { return ucp_worker_; }
  const UcpContext& context() const { return *ucp_context_; }

 private:
  ucp_worker_h ucp_worker_{nullptr};
  std::shared_ptr<UcpContext> ucp_context_;
};

class UcxStatusDetail : public arrow::StatusDetail {
 public:
  explicit UcxStatusDetail(ucs_status_t status) : status_(status) {}
  static constexpr char const kTypeId[] = "ucx::UcxStatusDetail";

  const char* type_id() const override { return kTypeId; }
  std::string ToString() const override;
  static ucs_status_t Unwrap(const arrow::Status& status);

 private:
  ucs_status_t status_;
};

arrow::Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

class UcxDataBuffer : public arrow::Buffer {
 public:
  UcxDataBuffer(std::shared_ptr<UcpWorker> worker, void* data, const size_t size)
      : arrow::Buffer(reinterpret_cast<uint8_t*>(data), static_cast<int64_t>(size)),
        worker_(std::move(worker)) {}
  ~UcxDataBuffer() override {
    ucp_am_data_release(worker_->get(),
                        const_cast<void*>(reinterpret_cast<const void*>(data())));
  }

 private:
  std::shared_ptr<UcpWorker> worker_;
};

arrow::Result<size_t> to_sockaddr(const std::string& host, const int32_t port,
                                  struct sockaddr_storage* addr);
arrow::Result<std::string> SockaddrToString(const struct sockaddr_storage& address);

static inline bool is_ignorable_disconnect_error(ucs_status_t ucs_status) {
  // not connected, connection reset: we're already disconnected
  // timeout: most likely disconnected, but we can't tell from our end
  switch (ucs_status) {
    case UCS_OK:
    case UCS_ERR_ENDPOINT_TIMEOUT:
    case UCS_ERR_NOT_CONNECTED:
    case UCS_ERR_CONNECTION_RESET:
      return true;
  }
  return false;
}
}  // namespace utils
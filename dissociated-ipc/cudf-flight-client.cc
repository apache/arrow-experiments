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

#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/gpu/cuda_api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/endian.h>
#include <arrow/util/logging.h>
#include <arrow/util/uri.h>

#include "cudf-flight-ucx.h"
#include "ucx_client.h"

namespace flight = arrow::flight;
namespace ipc = arrow::ipc;

arrow::Result<ucp_tag_t> get_want_data_tag(const arrow::util::Uri& loc) {
  ARROW_ASSIGN_OR_RAISE(auto query_params, loc.query_items());
  for (auto& q : query_params) {
    if (q.first == "want_data") {
      return std::stoull(q.second);
    }
  }
  return 0;
}

// utility client class to read a stream of data using the dissociated ipc
// protocol structure
class StreamReader {
 public:
  StreamReader(utils::Connection* ctrl_cnxn, utils::Connection* data_cnxn)
      : ctrl_cnxn_{ctrl_cnxn}, data_cnxn_{data_cnxn} {
    ARROW_UNUSED(ctrl_cnxn_->SetAMHandler(0, this, RecvMsg));
  }

  void set_data_mem_manager(std::shared_ptr<arrow::MemoryManager> mgr) {
    if (!mgr) {
      mm_ = arrow::CPUDevice::Instance()->default_memory_manager();
    } else {
      mm_ = std::move(mgr);
    }
  }

  arrow::Status Start(ucp_tag_t ctrl_tag, ucp_tag_t data_tag, const std::string& ident) {
    // consume the data and metadata streams simultaneously
    ARROW_RETURN_NOT_OK(ctrl_cnxn_->SendTagSync(ctrl_tag, ident.data(), ident.size()));
    ARROW_RETURN_NOT_OK(data_cnxn_->SendTagSync(data_tag, ident.data(), ident.size()));

    std::thread(&StreamReader::run_data_loop, this).detach();
    std::thread(&StreamReader::run_meta_loop, this).detach();

    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> Schema() {
    // return the schema if we've already pulled it
    if (schema_) {
      return schema_;
    }

    // otherwise the next message should be the schema
    ARROW_ASSIGN_OR_RAISE(auto msg, NextMsg());
    ARROW_ASSIGN_OR_RAISE(schema_, ipc::ReadSchema(*msg, &dictionary_memo_));
    return schema_;
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    // we need the schema to read the record batch, also ensuring that
    // we will retrieve the schema message which should be the first message
    ARROW_ASSIGN_OR_RAISE(auto schema, Schema());
    ARROW_ASSIGN_OR_RAISE(auto msg, NextMsg());
    if (msg) {
      return ipc::ReadRecordBatch(*msg, schema, &dictionary_memo_, ipc_options_);
    }
    // we've hit the end
    return nullptr;
  }

 protected:
  struct PendingMsg {
    std::promise<std::unique_ptr<ipc::Message>> p;
    std::shared_ptr<arrow::Buffer> metadata;
    std::shared_ptr<arrow::Buffer> body;
    StreamReader* rdr;
  };

  // data stream loop handler
  void run_data_loop() {
    if (arrow::cuda::IsCudaMemoryManager(*mm_)) {
      // since we're in a new thread, we need to make sure to push the cuda context
      // so that ucx uses the same cuda context as the Arrow data is using, otherwise
      // the device pointers aren't valid
      auto ctx = *(*arrow::cuda::AsCudaMemoryManager(mm_))->cuda_device()->GetContext();
      cuCtxPushCurrent(reinterpret_cast<CUcontext>(ctx->handle()));
    }

    while (true) {
      // progress the connection until an event happens
      while (data_cnxn_->Progress()) {
      }
      {
        // check if we have received any metadata which indicate we need to poll
        // for a corresponding tagged data message
        std::unique_lock<std::mutex> guard(polling_mutex_);
        for (auto it = polling_map_.begin(); it != polling_map_.end();) {
          auto maybe_tag =
              data_cnxn_->ProbeForTag(ucp_tag_t(it->first), 0x00000000FFFFFFFF, 1);
          if (!maybe_tag.ok()) {
            ARROW_LOG(ERROR) << maybe_tag.status().ToString();
            return;
          }

          auto tag_pair = maybe_tag.MoveValueUnsafe();
          if (tag_pair.second != nullptr) {
            // got one!
            auto st = RecvTag(tag_pair.second, tag_pair.first, std::move(it->second));
            if (!st.ok()) {
              ARROW_LOG(ERROR) << st.ToString();
              return;
            }
            it = polling_map_.erase(it);
          } else {
            ++it;
          }
        }
      }

      // if the metadata stream has ended...
      if (finished_metadata_.load()) {
        // we are done if there's nothing left to poll for and nothing outstanding
        std::lock_guard<std::mutex> guard(polling_mutex_);
        if (polling_map_.empty() && outstanding_tags_.load() == 0) {
          break;
        }
      }
    }
  }

  // a mask to grab the byte indicating the body message type.
  static constexpr uint64_t kbody_mask_ = 0x0100000000000000;

  arrow::Status RecvTag(ucp_tag_message_h msg, ucp_tag_recv_info_t info_tag,
                        PendingMsg pending) {
    ++outstanding_tags_;
    ARROW_ASSIGN_OR_RAISE(auto buf, mm_->AllocateBuffer(info_tag.length));

    PendingMsg* new_pending = new PendingMsg(std::move(pending));
    new_pending->body = std::move(buf);
    new_pending->rdr = this;
    return data_cnxn_->RecvTagData(
        msg, reinterpret_cast<void*>(new_pending->body->address()), info_tag.length,
        new_pending,
        [](void* request, ucs_status_t status, const ucp_tag_recv_info_t* tag_info,
           void* user_data) {
          auto pending =
              std::unique_ptr<PendingMsg>(reinterpret_cast<PendingMsg*>(user_data));
          if (status != UCS_OK) {
            ARROW_LOG(ERROR)
                << utils::FromUcsStatus("ucp_tag_recv_nbx_callback", status).ToString();
            pending->p.set_value(nullptr);
            return;
          }

          if (request) ucp_request_free(request);

          if (tag_info->sender_tag & kbody_mask_) {
            // pointer / offset list body
            // not yet implemented
          } else {
            // full body bytes, use the pending metadata and read our IPC message
            // as usual
            auto msg = *ipc::Message::Open(pending->metadata, pending->body);
            pending->p.set_value(std::move(msg));
            --pending->rdr->outstanding_tags_;
          }
        },
        (new_pending->body->is_cpu()) ? UCS_MEMORY_TYPE_HOST : UCS_MEMORY_TYPE_CUDA);
  }

  // handle the metadata stream
  void run_meta_loop() {
    while (!finished_metadata_.load()) {
      // progress the connection until we get an event
      while (ctrl_cnxn_->Progress()) {
      }
      {
        std::unique_lock<std::mutex> guard(queue_mutex_);
        while (!metadata_queue_.empty()) {
          // handle any metadata messages in our queue
          auto buf = std::move(metadata_queue_.front());
          metadata_queue_.pop();
          guard.unlock();

          while (buf.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
            ctrl_cnxn_->Progress();
          }

          std::shared_ptr<arrow::Buffer> buffer = buf.get();
          if (static_cast<MetadataMsgType>(buffer->data()[0]) == MetadataMsgType::EOS) {
            finished_metadata_.store(true);
            guard.lock();
            continue;
          }

          uint32_t sequence_number = utils::BytesToUint32LE(buffer->data() + 1);
          auto metadata = SliceBuffer(buffer, 5, buffer->size() - 5);

          // store a mapping of sequence numbers to std::future that returns the data
          std::promise<std::unique_ptr<ipc::Message>> p;
          {
            std::lock_guard<std::mutex> lock(msg_mutex_);
            msg_map_.insert({sequence_number, p.get_future()});
          }
          cv_progress_.notify_all();

          auto msg = ipc::Message::Open(metadata, nullptr).ValueOrDie();
          if (!ipc::Message::HasBody(msg->type())) {
            p.set_value(std::move(msg));
            guard.lock();
            continue;
          }

          {
            std::lock_guard<std::mutex> lock(polling_mutex_);
            polling_map_.insert(
                {sequence_number, PendingMsg{std::move(p), std::move(metadata)}});
          }

          guard.lock();
        }
      }

      if (finished_metadata_.load()) break;
      auto status = utils::FromUcsStatus("ucp_worker_wait", ctrl_cnxn_->WorkerWait());
      if (!status.ok()) {
        ARROW_LOG(ERROR) << status.ToString();
        return;
      }
    }
  }

  arrow::Result<std::unique_ptr<ipc::Message>> NextMsg() {
    // fetch the next IPC message by sequence number
    const uint32_t counter = next_counter_++;
    std::future<std::unique_ptr<ipc::Message>> futr;
    {
      std::unique_lock<std::mutex> lock(msg_mutex_);
      if (msg_map_.empty() && finished_metadata_.load() && !outstanding_tags_.load()) {
        return nullptr;
      }

      auto it = msg_map_.find(counter);
      if (it == msg_map_.end()) {
        // wait until we get a message for this sequence number
        cv_progress_.wait(lock, [this, counter, &it] {
          it = msg_map_.find(counter);
          return it != msg_map_.end() || finished_metadata_.load();
        });
      }
      futr = std::move(it->second);
      msg_map_.erase(it);
    }

    // .get on a future will block until it either recieves a value or fails
    return futr.get();
  }

  // callback function to recieve untagged "Active Messages"
  static ucs_status_t RecvMsg(void* arg, const void* header, size_t header_len,
                              void* data, size_t length,
                              const ucp_am_recv_param_t* param) {
    StreamReader* rdr = reinterpret_cast<StreamReader*>(arg);
    DCHECK(length);

    std::promise<std::unique_ptr<arrow::Buffer>> p;
    {
      std::lock_guard<std::mutex> lock(rdr->queue_mutex_);
      rdr->metadata_queue_.push(p.get_future());
    }

    return rdr->ctrl_cnxn_->RecvAM(std::move(p), header, header_len, data, length, param);
  }

 private:
  utils::Connection* ctrl_cnxn_;
  utils::Connection* data_cnxn_;
  std::shared_ptr<arrow::Schema> schema_;
  ipc::DictionaryMemo dictionary_memo_;
  ipc::IpcReadOptions ipc_options_;

  std::shared_ptr<arrow::MemoryManager> mm_;
  std::atomic<bool> finished_metadata_{false};
  std::atomic<uint32_t> outstanding_tags_{0};
  uint32_t next_counter_{0};

  std::condition_variable cv_progress_;
  std::mutex queue_mutex_;
  std::queue<std::future<std::unique_ptr<arrow::Buffer>>> metadata_queue_;
  std::mutex polling_mutex_;
  std::unordered_map<uint32_t, PendingMsg> polling_map_;
  std::mutex msg_mutex_;
  std::unordered_map<uint32_t, std::future<std::unique_ptr<ipc::Message>>> msg_map_;
};

arrow::Status run_client(const std::string& addr, const int port) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp(addr, port));
  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location));

  ARROW_ASSIGN_OR_RAISE(
      auto info,
      client->GetFlightInfo(flight::FlightDescriptor::Command("train.parquet")));
  ARROW_LOG(DEBUG) << info->endpoints()[0].locations[0].ToString();
  ARROW_LOG(DEBUG) << info->endpoints()[0].locations[1].ToString();

  ARROW_ASSIGN_OR_RAISE(auto ctrl_uri, arrow::util::Uri::FromString(
                                           info->endpoints()[0].locations[0].ToString()));
  ARROW_ASSIGN_OR_RAISE(auto data_uri, arrow::util::Uri::FromString(
                                           info->endpoints()[0].locations[1].ToString()));

  ARROW_ASSIGN_OR_RAISE(ucp_tag_t ctrl_tag, get_want_data_tag(ctrl_uri));
  ARROW_ASSIGN_OR_RAISE(ucp_tag_t data_tag, get_want_data_tag(data_uri));
  const std::string& ident = info->endpoints()[0].ticket.ticket;

  ARROW_ASSIGN_OR_RAISE(auto cuda_mgr, arrow::cuda::CudaDeviceManager::Instance());
  ARROW_ASSIGN_OR_RAISE(auto device, cuda_mgr->GetDevice(0));
  ARROW_ASSIGN_OR_RAISE(auto cuda_device, arrow::cuda::AsCudaDevice(device));
  ARROW_ASSIGN_OR_RAISE(auto ctx, cuda_device->GetContext());
  cuCtxPushCurrent(reinterpret_cast<CUcontext>(ctx->handle()));

  ARROW_LOG(DEBUG) << device->ToString();

  UcxClient ctrl_client, data_client;
  ARROW_RETURN_NOT_OK(ctrl_client.Init(ctrl_uri.host(), ctrl_uri.port()));
  ARROW_RETURN_NOT_OK(data_client.Init(data_uri.host(), data_uri.port()));

  ARROW_ASSIGN_OR_RAISE(auto ctrl_cnxn, ctrl_client.CreateConn());
  ARROW_ASSIGN_OR_RAISE(auto data_cnxn, data_client.CreateConn());

  StreamReader rdr(ctrl_cnxn.get(), data_cnxn.get());
  rdr.set_data_mem_manager(ctx->memory_manager());

  ARROW_RETURN_NOT_OK(rdr.Start(ctrl_tag, data_tag, ident));

  ARROW_ASSIGN_OR_RAISE(auto s, rdr.Schema());
  std::cout << s->ToString() << std::endl;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, rdr.Next());
    if (!batch) {
      break;
    }

    std::cout << batch->num_columns() << " " << batch->num_rows() << std::endl;
    std::cout << batch->column(0)->data()->buffers[1]->device()->ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto cpubatch,
                          batch->CopyTo(arrow::default_cpu_memory_manager()));
    std::cout << cpubatch->ToString() << std::endl;
  }

  ARROW_CHECK_OK(ctrl_cnxn->Close());
  ARROW_CHECK_OK(data_cnxn->Close());
  return arrow::Status::OK();
}

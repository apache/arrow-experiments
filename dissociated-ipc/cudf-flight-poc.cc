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
#include <queue>
#include <thread>

#include <gflags/gflags.h>
#include <cudf/interop.hpp>
#include <cudf/io/parquet.hpp>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/device.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/gpu/cuda_api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/endian.h>
#include <arrow/util/logging.h>
#include <arrow/util/uri.h>

#include "ucx_client.h"
#include "ucx_server.h"

namespace flight = arrow::flight;
namespace ipc = arrow::ipc;

// Define some constants for the `want_data` tags
static constexpr ucp_tag_t kWantDataTag = 0x00000DEADBA0BAB0;
static constexpr ucp_tag_t kWantCtrlTag = 0xFFFFFDEADBA0BAB0;
// define a mask to check the tag
static constexpr ucp_tag_t kWantCtrlMask = 0xFFFFF00000000000;

enum class MetadataMsgType : uint8_t {
  EOS = 0,
  METADATA = 1,
};

cudf::column_metadata column_info_to_metadata(const cudf::io::column_name_info& info) {
  cudf::column_metadata result;
  result.name = info.name;
  std::transform(info.children.begin(), info.children.end(),
                 std::back_inserter(result.children_meta), column_info_to_metadata);
  return result;
}

std::vector<cudf::column_metadata> table_metadata_to_column(
    const cudf::io::table_metadata& tbl_meta) {
  std::vector<cudf::column_metadata> result;

  std::transform(tbl_meta.schema_info.begin(), tbl_meta.schema_info.end(),
                 std::back_inserter(result), column_info_to_metadata);
  return result;
}

// a UCX server which serves cuda record batches via the dissociated ipc protocol
class CudaUcxServer : public UcxServer {
 public:
  CudaUcxServer() {
    // create a buffer holding 8 bytes on the GPU to use for padding buffers
    cuda_padding_bytes_ = rmm::device_buffer(8, rmm::cuda_stream_view{});
    cuMemsetD8(reinterpret_cast<uintptr_t>(cuda_padding_bytes_.data()), 0, 8);
  }
  virtual ~CudaUcxServer() {
    if (listening_.load()) {
      ARROW_UNUSED(Shutdown());
    }
  }

  arrow::Status initialize() {
    // load the parquet data directly onto the GPU as a libcudf table
    auto source = cudf::io::source_info("./data/taxi-data/train.parquet");
    auto options = cudf::io::parquet_reader_options::builder(source);
    cudf::io::chunked_parquet_reader rdr(1 * 1024 * 1024, options);

    // get arrow::RecordBatches for each chunk of the parquet data while
    // leaving the data on the GPU
    arrow::RecordBatchVector batches;
    auto chunk = rdr.read_chunk();
    auto schema = cudf::to_arrow_schema(chunk.tbl->view(),
                                        table_metadata_to_column(chunk.metadata));
    auto device_out = cudf::to_arrow_device(std::move(*chunk.tbl));
    ARROW_ASSIGN_OR_RAISE(auto data,
                          arrow::ImportDeviceRecordBatch(device_out.get(), schema.get()));

    batches.push_back(std::move(data));

    while (rdr.has_next()) {
      chunk = rdr.read_chunk();
      device_out = cudf::to_arrow_device(std::move(*chunk.tbl));
      ARROW_ASSIGN_OR_RAISE(
          data, arrow::ImportDeviceRecordBatch(device_out.get(), schema.get()));
      batches.push_back(std::move(data));
    }

    data_sets_.emplace("train.parquet", std::move(batches));

    // initialize the server and let it choose its own port
    ARROW_RETURN_NOT_OK(Init("127.0.0.1", 0));

    ARROW_ASSIGN_OR_RAISE(ctrl_location_,
                          flight::Location::Parse(location_.ToString() + "?want_data=" +
                                                  std::to_string(kWantCtrlTag)));
    ARROW_ASSIGN_OR_RAISE(data_location_,
                          flight::Location::Parse(location_.ToString() + "?want_data=" +
                                                  std::to_string(kWantDataTag)));
    return arrow::Status::OK();
  }

  inline flight::Location ctrl_location() const { return ctrl_location_; }
  inline flight::Location data_location() const { return data_location_; }

 protected:
  arrow::Status setup_handlers(UcxServer::ClientWorker* worker) override {
    return arrow::Status::OK();
  }

  arrow::Status do_work(UcxServer::ClientWorker* worker) override {
    // probe for a message with the want_data tag synchronously,
    // so this will block until it receives a message with this tag
    ARROW_ASSIGN_OR_RAISE(
        auto tag_info, worker->conn_->ProbeForTagSync(kWantDataTag, ~kWantCtrlMask, 1));

    std::string msg;
    msg.resize(tag_info.first.length);
    ARROW_RETURN_NOT_OK(
        worker->conn_->RecvTagData(tag_info.second, reinterpret_cast<void*>(msg.data()),
                                   msg.size(), nullptr, nullptr, UCS_MEMORY_TYPE_HOST));

    ARROW_LOG(DEBUG) << "server received WantData: " << msg;

    // simulate two separate servers, one metadata server and one body data server
    if (tag_info.first.sender_tag & kWantCtrlMask) {
      return send_metadata_stream(worker, msg);
    }

    return send_data_stream(worker, msg);
  }

 private:
  arrow::Status send_metadata_stream(UcxServer::ClientWorker* worker,
                                     const std::string& ident) {
    auto it = data_sets_.find(ident);
    if (it == data_sets_.end()) {
      return arrow::Status::Invalid("data set not found:", ident);
    }

    ipc::IpcWriteOptions ipc_options;
    ipc::DictionaryFieldMapper mapper;
    const auto& record_list = it->second;
    auto schema = record_list[0]->schema();
    ARROW_RETURN_NOT_OK(mapper.AddSchemaFields(*schema));

    // for each record in the stream, collect the IPC metadata to send
    uint32_t sequence_num = 0;
    // schema payload is first
    ipc::IpcPayload payload;
    ARROW_RETURN_NOT_OK(ipc::GetSchemaPayload(*schema, ipc_options, mapper, &payload));
    ARROW_RETURN_NOT_OK(write_ipc_metadata(worker->conn_.get(), payload, sequence_num++));

    // then any dictionaries
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries,
                          ipc::CollectDictionaries(*record_list[0], mapper));
    for (const auto& pair : dictionaries) {
      ARROW_RETURN_NOT_OK(
          ipc::GetDictionaryPayload(pair.first, pair.second, ipc_options, &payload));
      ARROW_RETURN_NOT_OK(
          write_ipc_metadata(worker->conn_.get(), payload, sequence_num++));
    }

    // finally the record batch metadata messages
    for (const auto& batch : record_list) {
      ARROW_RETURN_NOT_OK(ipc::GetRecordBatchPayload(*batch, ipc_options, &payload));
      ARROW_RETURN_NOT_OK(
          write_ipc_metadata(worker->conn_.get(), payload, sequence_num++));
    }

    // finally, we send the End-Of-Stream message
    std::array<uint8_t, 5> eos_bytes{static_cast<uint8_t>(MetadataMsgType::EOS), 0, 0, 0,
                                     0};
    utils::Uint32ToBytesLE(sequence_num, eos_bytes.data() + 1);

    ARROW_RETURN_NOT_OK(worker->conn_->Flush());
    return worker->conn_->SendAM(0, eos_bytes.data(), eos_bytes.size());
  }

  struct PendingIOV {
    std::vector<ucp_dt_iov_t> iovs;
    arrow::BufferVector body_buffers;
  };

  arrow::Status write_ipc_metadata(utils::Connection* cnxn,
                                   const ipc::IpcPayload& payload,
                                   const uint32_t sequence_num) {
    // our metadata messages are always CPU host memory
    ucs_memory_type_t mem_type = UCS_MEMORY_TYPE_HOST;

    // construct our 5 byte prefix, the message type followed by the sequence number
    auto pending = std::make_unique<PendingIOV>();
    pending->iovs.resize(2);
    pending->iovs[0].buffer = malloc(5);
    pending->iovs[0].length = 5;
    reinterpret_cast<uint8_t*>(pending->iovs[0].buffer)[0] =
        static_cast<uint8_t>(MetadataMsgType::METADATA);
    utils::Uint32ToBytesLE(sequence_num,
                           reinterpret_cast<uint8_t*>(pending->iovs[0].buffer) + 1);

    // after the prefix, we add the metadata we want to send
    pending->iovs[1].buffer = const_cast<void*>(payload.metadata->data_as<void>());
    pending->iovs[1].length = payload.metadata->size();
    pending->body_buffers.emplace_back(payload.metadata);

    auto* pending_iov = pending.get();
    void* user_data = pending.release();
    return cnxn->SendAMIov(
        0, pending_iov->iovs.data(), pending_iov->iovs.size(), user_data,
        [](void* request, ucs_status_t status, void* user_data) {
          auto pending_iov =
              std::unique_ptr<PendingIOV>(reinterpret_cast<PendingIOV*>(user_data));
          if (request) ucp_request_free(request);
          if (status != UCS_OK) {
            ARROW_LOG(ERROR)
                << utils::FromUcsStatus("ucp_am_send_nbx_cb", status).ToString();
          }
          free(pending_iov->iovs[0].buffer);
        },
        mem_type);
  }

  arrow::Status send_data_stream(UcxServer::ClientWorker* worker,
                                 const std::string& ident) {
    auto it = data_sets_.find(ident);
    if (it == data_sets_.end()) {
      return arrow::Status::Invalid("data set not found:", ident);
    }

    ipc::IpcWriteOptions ipc_options;
    ipc::DictionaryFieldMapper mapper;
    const auto& record_list = it->second;
    auto schema = record_list[0]->schema();
    ARROW_RETURN_NOT_OK(mapper.AddSchemaFields(*schema));

    // start at 1 since schema payload has no body
    uint32_t sequence_num = 1;

    ipc::IpcPayload payload;
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries,
                          ipc::CollectDictionaries(*record_list[0], mapper));
    for (const auto& pair : dictionaries) {
      ARROW_RETURN_NOT_OK(
          ipc::GetDictionaryPayload(pair.first, pair.second, ipc_options, &payload));
      ARROW_RETURN_NOT_OK(write_ipc_body(worker->conn_.get(), payload, sequence_num++));
    }

    for (const auto& batch : record_list) {
      ARROW_RETURN_NOT_OK(ipc::GetRecordBatchPayload(*batch, ipc_options, &payload));
      ARROW_RETURN_NOT_OK(write_ipc_body(worker->conn_.get(), payload, sequence_num++));
    }

    return worker->conn_->Flush();
  }

  arrow::Status write_ipc_body(utils::Connection* cnxn, const ipc::IpcPayload& payload,
                               const uint32_t sequence_num) {
    ucs_memory_type_t mem_type = UCS_MEMORY_TYPE_CUDA;

    // determine the number of buffers and padding we need along with the total size
    auto pending = std::make_unique<PendingIOV>();
    int32_t total_buffers = 0;
    for (const auto& buffer : payload.body_buffers) {
      if (!buffer || buffer->size() == 0) continue;
      if (buffer->is_cpu()) {
        mem_type = UCS_MEMORY_TYPE_HOST;
      }
      total_buffers++;
      // arrow ipc requires aligning buffers to 8 byte boundary
      const auto remainder = static_cast<int>(
          arrow::bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
      if (remainder) total_buffers++;
    }

    pending->iovs.resize(total_buffers);
    // we'll use scatter-gather to avoid extra copies
    ucp_dt_iov_t* iov = pending->iovs.data();
    pending->body_buffers = payload.body_buffers;

    void* padding_bytes =
        const_cast<void*>(reinterpret_cast<const void*>(padding_bytes_.data()));
    if (mem_type == UCS_MEMORY_TYPE_CUDA) {
      padding_bytes = cuda_padding_bytes_.data();
    }

    for (const auto& buffer : payload.body_buffers) {
      if (!buffer || buffer->size() == 0) continue;
      // for cuda memory, buffer->address() will return a device pointer
      iov->buffer = const_cast<void*>(reinterpret_cast<const void*>(buffer->address()));
      iov->length = buffer->size();
      ++iov;

      const auto remainder = static_cast<int>(
          arrow::bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
      if (remainder) {
        iov->buffer = padding_bytes;
        iov->length = remainder;
        ++iov;
      }
    }

    auto pending_iov = pending.release();
    // indicate that we're sending the full data body, not a pointer list and add
    // the sequence number to the tag
    ucp_tag_t tag = (uint64_t(0) << 55) | arrow::bit_util::ToLittleEndian(sequence_num);
    return cnxn->SendTagIov(
        tag, pending_iov->iovs.data(), pending_iov->iovs.size(), pending_iov,
        [](void* request, ucs_status_t status, void* user_data) {
          auto pending_iov =
              std::unique_ptr<PendingIOV>(reinterpret_cast<PendingIOV*>(user_data));
          if (status != UCS_OK) {
            ARROW_LOG(ERROR)
                << utils::FromUcsStatus("ucp_tag_send_nbx_cb", status).ToString();
          }
          if (request) {
            ucp_request_free(request);
          }
        },
        mem_type);
  }

  flight::Location ctrl_location_;
  flight::Location data_location_;
  std::unordered_map<std::string, arrow::RecordBatchVector> data_sets_;

  rmm::device_buffer cuda_padding_bytes_;
  const std::array<uint8_t, 8> padding_bytes_{0, 0, 0, 0, 0, 0, 0, 0};
};

// a flight server that will serve up the flight-info with a ucx uri to point
// to the cuda ucx server
class CudaFlightServer : public flight::FlightServerBase {
 public:
  CudaFlightServer(flight::Location ctrl_server_loc, flight::Location data_server_loc)
      : FlightServerBase(),
        ctrl_server_loc_{std::move(ctrl_server_loc)},
        data_server_loc_{std::move(data_server_loc)} {}
  ~CudaFlightServer() override {}

  inline void register_schema(std::string cmd, std::shared_ptr<arrow::Schema> schema) {
    schema_reg_.emplace(std::move(cmd), std::move(schema));
  }

  arrow::Status GetFlightInfo(const flight::ServerCallContext& context,
                              const flight::FlightDescriptor& request,
                              std::unique_ptr<flight::FlightInfo>* info) override {
    flight::FlightEndpoint endpoint{
        {request.cmd}, {ctrl_server_loc_, data_server_loc_}, std::nullopt, {}};

    auto it = schema_reg_.find(request.cmd);
    if (it == schema_reg_.end() || !it->second) {
      return arrow::Status::Invalid("could not find schema for ", request.cmd);
    }

    ARROW_ASSIGN_OR_RAISE(
        auto flightinfo,
        flight::FlightInfo::Make(*it->second, request, {endpoint}, -1, -1, false));
    *info = std::make_unique<flight::FlightInfo>(std::move(flightinfo));
    return arrow::Status::OK();
  }

 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schema_reg_;
  flight::Location ctrl_server_loc_;
  flight::Location data_server_loc_;
};

arrow::Status run_server(const std::string& addr, const int port) {
  CudaUcxServer server;
  ARROW_ASSIGN_OR_RAISE(auto mgr, arrow::cuda::CudaDeviceManager::Instance());
  ARROW_ASSIGN_OR_RAISE(auto ctx, mgr->GetContext(0));
  server.set_cuda_context(ctx);
  ARROW_RETURN_NOT_OK(server.initialize());

  flight::Location ctrl_server_location = server.ctrl_location();
  flight::Location data_server_location = server.data_location();

  std::shared_ptr<arrow::Schema> sc;
  {
    auto source = cudf::io::source_info("./data/taxi-data/train.parquet");
    auto options = cudf::io::parquet_reader_options::builder(source).num_rows(1);
    auto result = cudf::io::read_parquet(options);

    auto schema = cudf::to_arrow_schema(result.tbl->view(),
                                        table_metadata_to_column(result.metadata));
    ARROW_ASSIGN_OR_RAISE(sc, arrow::ImportSchema(schema.get()));
  }

  auto flight_server = std::make_shared<CudaFlightServer>(
      std::move(ctrl_server_location), std::move(data_server_location));
  flight_server->register_schema("train.parquet", std::move(sc));

  ARROW_ASSIGN_OR_RAISE(auto loc, flight::Location::ForGrpcTcp(addr, port));
  flight::FlightServerOptions options(loc);

  RETURN_NOT_OK(flight_server->Init(options));
  RETURN_NOT_OK(flight_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Flight Server Listening on " << flight_server->location().ToString()
            << std::endl;

  return flight_server->Serve();
}

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
  ARROW_RETURN_NOT_OK(
      ctrl_client.Init(ctrl_uri.host(), ctrl_uri.port()));
  ARROW_RETURN_NOT_OK(
      data_client.Init(data_uri.host(), data_uri.port()));

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

DEFINE_int32(port, 31337, "port to listen or connect");
DEFINE_string(address, "127.0.0.1", "address to connect to");
DEFINE_bool(client, false, "run the client");

int main(int argc, char** argv) {
  arrow::util::ArrowLog::StartArrowLog("cudf-flight-poc",
                                       arrow::util::ArrowLogLevel::ARROW_DEBUG);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // std::thread t1(run_server, FLAGS_address, FLAGS_port);

  // using namespace std::chrono_literals;
  // std::this_thread::sleep_for(10000ms);
  // std::thread t2(run_client, FLAGS_address, FLAGS_port);

  // t2.join();
  // t1.join();
  if (FLAGS_client) {
    ARROW_CHECK_OK(run_client(FLAGS_address, FLAGS_port));
  } else {
    ARROW_CHECK_OK(run_server(FLAGS_address, FLAGS_port));
  }
}

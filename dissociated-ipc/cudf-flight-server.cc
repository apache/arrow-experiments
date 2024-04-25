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

#include <cudf/interop.hpp>
#include <cudf/io/parquet.hpp>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/server.h>
#include <arrow/gpu/cuda_api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/endian.h>
#include <arrow/util/logging.h>
#include <arrow/util/uri.h>

#include "cudf-flight-ucx.h"
#include "ucx_server.h"

namespace flight = arrow::flight;
namespace ipc = arrow::ipc;

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
    ucp_tag_t tag =
        (uint64_t(0) << kShiftBodyType) | arrow::bit_util::ToLittleEndian(sequence_num);
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
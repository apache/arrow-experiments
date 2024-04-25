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

// Define some constants for the `want_data` tags
static constexpr ucp_tag_t kWantDataTag = 0x00000DEADBA0BAB0;
static constexpr ucp_tag_t kWantCtrlTag = 0xFFFFFDEADBA0BAB0;
// define a mask to check the tag
static constexpr ucp_tag_t kWantCtrlMask = 0xFFFFF00000000000;

// constant for the bit shift to make the data body type the most
// significant byte
static constexpr int kShiftBodyType = 55;

enum class MetadataMsgType : uint8_t {
  EOS = 0,
  METADATA = 1,
};

arrow::Status run_server(const std::string& addr, const int port);
arrow::Status run_client(const std::string& addr, const int port);
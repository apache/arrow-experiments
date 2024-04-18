# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.20)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")
project(arrow-cudf-flight CXX CUDA)

include(CMakeParseArguments)

# https://www.cmake.org/cmake/help/latest/policy/CMP0025.html
#
# Compiler id for Apple Clang is now AppleClang.
cmake_policy(SET CMP0025 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0042.html
#
# Enable MACOSX_RPATH by default. @rpath in a target's install name is
# a more flexible and powerful mechanism than @executable_path or
# @loader_path for locating shared libraries.
cmake_policy(SET CMP0042 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0054.html
#
# Only interpret if() arguments as variables or keywords when unquoted.
cmake_policy(SET CMP0054 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0057.html
#
# Support new if() IN_LIST operator.
cmake_policy(SET CMP0057 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0063.html
#
# Adapted from Apache Kudu: https://github.com/apache/kudu/commit/bd549e13743a51013585
# Honor visibility properties for all target types.
cmake_policy(SET CMP0063 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0068.html
#
# RPATH settings on macOS do not affect install_name.
cmake_policy(SET CMP0068 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0074.html
#
# find_package() uses <PackageName>_ROOT variables.
cmake_policy(SET CMP0074 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0091.html
#
# MSVC runtime library flags are selected by an abstraction.
cmake_policy(SET CMP0091 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0135.html
#
# CMP0135 is for solving re-building and re-downloading.
# We don't have a real problem with the OLD behavior for now
# but we use the NEW behavior explicitly to suppress CMP0135
# warnings.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

find_package(ArrowFlight REQUIRED)
find_package(ArrowCUDA REQUIRED)
message(STATUS "Found Arrow: ${ARROW_VERSION}")

find_package(CUDA QUIET REQUIRED)
find_package(gflags REQUIRED)
find_package(cudf REQUIRED)
find_package(ucx REQUIRED)
add_library(ucx::ucx INTERFACE IMPORTED)
target_link_libraries(ucx::ucx INTERFACE ucx::ucp ucx::uct ucx::ucs)

add_definitions(-DFMT_USE_NONTYPE_TEMPLATE_ARGS=0)
add_executable(arrow-cudf-flight                 
                cudf-flight-poc.cc
                ucx_utils.cc
                ucx_server.cc
                ucx_client.cc
                ucx_conn.cc)
target_link_libraries(arrow-cudf-flight 
                        arrow_shared arrow_cuda_shared 
                        arrow_flight_shared gflags 
                        cudf::cudf ucx::ucx)
set_target_properties(
  arrow-cudf-flight 
  PROPERTIES BUILD_RPATH "\$ORIGIN"
             INSTALL_RPATH "\$ORIGIN"
             CXX_STANDARD 20
             CXX_STANDARD_REQUIRED ON
             CXX_EXTENSIONS ON
             CUDA_STANDARD 20
             CUDA_STANDARD_REQUIRED ON             
)
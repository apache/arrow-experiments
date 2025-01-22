% Licensed to the Apache Software Foundation (ASF) under one
% or more contributor license agreements.  See the NOTICE file
% distributed with this work for additional information
% regarding copyright ownership.  The ASF licenses this file
% to you under the Apache License, Version 2.0 (the
% "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.

% The address of the HTTP server that
% returns Arrow IPC Stream responses.
server = "http://localhost:8008";

% Diagnostic output.
disp("Reading Arrow IPC Stream from " + server + "...");

% Start timing.
tic;

% Make an HTTP GET request to the local server
% to fetch an Arrow IPC Stream and read all the
% data into memory as a byte (uint8) array.
options = weboptions(ContentType="binary");
bytes = webread(server, options);

% Construct an Arrow RecordBatchStreamReader from the in-memory bytes.
reader = arrow.io.ipc.RecordBatchStreamReader.fromBytes(bytes);

% Read an Arrow table from the in-memory bytes.
arrowTable = reader.readTable();

% Stop timing.
time = toc;
% Round elapsed time to two decimal places.
time = round(time, 2);

% Number of bytes received.
nbytes = length(bytes);

% Diagnostic output.
disp("DONE ✔");
disp("---------------");
disp("Results")
disp("---------------");
disp("Time (s): " + sprintf("%.2f", time));
disp("Num Bytes: " + string(nbytes));
disp("Num Rows:" + string(arrowTable.NumRows));
disp("Num Columns:" + string(arrowTable.NumColumns));

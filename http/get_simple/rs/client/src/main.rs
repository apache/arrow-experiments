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

use arrow_ipc::reader::StreamReader;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
};
use tracing::{error, info, info_span};
use tracing_subscriber::fmt::format::FmtSpan;

fn main() {
    // Configure tracing subscriber.
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    info_span!("get_simple").in_scope(|| {
        // Connect to server.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8008);
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                info_span!("Reading Arrow IPC stream", %addr).in_scope(|| {
                    info!("Connected");

                    // Send request.
                    stream
                        .write_all(format!("GET / HTTP/1.1\r\nHost: {addr}\r\n\r\n").as_bytes())
                        .unwrap();

                    // Ignore response header.
                    let mut reader = BufReader::new(&mut stream);
                    let mut chunked = false;
                    loop {
                        let mut line = String::default();
                        reader.read_line(&mut line).unwrap();
                        if let Some(("transfer-encoding", "chunked")) = line
                            .to_lowercase()
                            .split_once(':')
                            .map(|(key, value)| (key.trim(), value.trim()))
                        {
                            chunked = true;
                        }
                        if line == "\r\n" {
                            break;
                        }
                    }

                    // Read Arrow IPC stream
                    let batches: Vec<_> = if chunked {
                        let mut buffer = Vec::default();
                        loop {
                            // Chunk size
                            let mut line = String::default();
                            reader.read_line(&mut line).unwrap();
                            let chunk_size = u64::from_str_radix(line.trim(), 16).unwrap();

                            if chunk_size == 0 {
                                // Terminating chunk
                                break;
                            } else {
                                // Append chunk to buffer
                                let mut chunk_reader = reader.take(chunk_size);
                                chunk_reader.read_to_end(&mut buffer).unwrap();
                                // Terminating CR-LF sequence
                                reader = chunk_reader.into_inner();
                                reader.read_line(&mut String::default()).unwrap();
                            }
                        }
                        StreamReader::try_new_unbuffered(buffer.as_slice(), None)
                            .unwrap()
                            .flat_map(Result::ok)
                            .collect()
                    } else {
                        StreamReader::try_new_unbuffered(reader, None)
                            .unwrap()
                            .flat_map(Result::ok)
                            .collect()
                    };

                    info!(
                        batches = batches.len(),
                        rows = batches.iter().map(|rb| rb.num_rows()).sum::<usize>()
                    );
                });
            }
            Err(error) => {
                error!(%error, "Connection failed")
            }
        }
    })
}

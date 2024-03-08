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
    io::{BufRead, BufReader, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
};
use tracing::{error, info, info_span};
use tracing_subscriber::fmt::format::FmtSpan;

fn main() {
    // Configure tracing subscriber.
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // Connect to server.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);
    match TcpStream::connect(addr) {
        Ok(mut stream) => {
            info_span!("Reading Arrow IPC stream", %addr).in_scope(|| {
                info!("Connected");

                // Send request.
                stream
                    .write_all("GET / HTTP/1.0\r\n\r\n".as_bytes())
                    .unwrap();

                // Ignore response header.
                for _ in BufReader::new(&mut stream)
                    .lines()
                    .take_while(|line| line.as_ref().is_ok_and(|line| !line.is_empty()))
                {
                }

                // Read Arrow IPC stream
                let reader = StreamReader::try_new_unbuffered(stream, None).unwrap();
                let batches = reader.flat_map(Result::ok).collect::<Vec<_>>();

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
}

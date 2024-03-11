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

use std::{
    io::{BufRead, BufReader, Result, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    sync::Arc,
    thread,
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Fields, Schema};
use once_cell::sync::Lazy;
use rand::{distributions::Standard, prelude::*};
use rayon::{iter, prelude::*};
use tracing::{error, info, info_span};
use tracing_subscriber::fmt::format::FmtSpan;

const RECORDS_PER_BATCH: usize = 4096;
const TOTAL_RECORDS: usize = if cfg!(debug_assertions) {
    100_000
} else {
    100_000_000
};

/// Schema for random data
static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        ('a'..='d')
            .map(|field_name| Field::new(field_name, DataType::Int64, true))
            .collect::<Fields>(),
    ))
});

/// Random data
static DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    info_span!("data", TOTAL_RECORDS, RECORDS_PER_BATCH).in_scope(|| {
        info!("Generating random data");
        // Generate recordbatches with random data
        iter::repeatn(
            RECORDS_PER_BATCH,
            TOTAL_RECORDS.div_euclid(RECORDS_PER_BATCH),
        )
        .chain(iter::once(TOTAL_RECORDS.rem_euclid(RECORDS_PER_BATCH)))
        .map_init(rand::thread_rng, |rng, len| {
            RecordBatch::try_new(
                Arc::clone(&SCHEMA),
                (0..SCHEMA.all_fields().len())
                    .map(|_| {
                        Arc::new(
                            rng.sample_iter::<i64, Standard>(Standard)
                                .take(len)
                                .collect::<Int64Array>(),
                        ) as _
                    })
                    .collect(),
            )
        })
        .flatten()
        .collect()
    })
});

fn get_simple(mut stream: std::net::TcpStream) {
    info!("Incoming connection");

    // Ignore incoming request.
    for _ in BufReader::new(&mut stream)
        .lines()
        .take_while(|line| line.as_ref().is_ok_and(|line| !line.is_empty()))
    {}

    // Write response header.
    stream
        .write_all(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.apache.arrow.stream\r\n\r\n"
                .as_bytes(),
        )
        .unwrap();

    // Stream the body.
    let mut writer = StreamWriter::try_new(stream, &SCHEMA).unwrap();
    for batch in DATA.iter() {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();

    let stream = writer.into_inner().unwrap();
    stream.shutdown(std::net::Shutdown::Both).unwrap();
}

fn main() -> Result<()> {
    // Configure tracing subscriber.
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // Generate random data.
    let _ = Lazy::force(&DATA);

    // Start listening.
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);
    let listener = TcpListener::bind(bind_addr)?;
    info!(%bind_addr, "Listening");

    // Handle incoming connections.
    loop {
        match listener.accept() {
            Ok((stream, remote_peer)) => {
                thread::spawn(move || {
                    info_span!("Writing Arrow IPC stream", %remote_peer)
                        .in_scope(|| get_simple(stream))
                });
            }
            Err(error) => {
                error!(%error, "Connection failed");
            }
        }
    }
}

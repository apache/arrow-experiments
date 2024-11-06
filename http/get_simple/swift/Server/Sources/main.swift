/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation
import Arrow
import Hummingbird

func makeRecordBatch(_ numRecords: UInt32) throws -> RecordBatch {
    let doubleBuilder: NumberArrayBuilder<Double> = try ArrowArrayBuilders.loadNumberArrayBuilder()
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    let date32Builder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    for idx in 0..<numRecords {
        doubleBuilder.append(11.11 * Double(idx))
        stringBuilder.append("test\(idx)")
        if (idx & 1) == 1 {
            date32Builder.append(date1)
        } else {
            date32Builder.append(date2)
        }
    }

    let doubleHolder = ArrowArrayHolderImpl(try doubleBuilder.finish())
    let stringHolder = ArrowArrayHolderImpl(try stringBuilder.finish())
    let date32Holder = ArrowArrayHolderImpl(try date32Builder.finish())
    let result = RecordBatch.Builder()
        .addColumn("col1", arrowArray: doubleHolder)
        .addColumn("col2", arrowArray: stringHolder)
        .addColumn("col3", arrowArray: date32Holder)
        .finish()
    switch result {
    case .success(let recordBatch):
        return recordBatch
    case .failure(let error):
        throw error
    }
}

let router = Router()
router.get("/") { request, _ -> ByteBuffer in
    print("received request from client")
    let recordBatchs = [try makeRecordBatch(4), try makeRecordBatch(3)]
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(.recordbatch, schema: recordBatchs[0].schema, batches: recordBatchs)
    switch arrowWriter.toStream(writerInfo) {
    case .success(let writeData):
        print("sending recordBatchs: \(recordBatchs.count)")
        return ByteBuffer(data: writeData)
    case.failure(let error):
        throw error
    }
}

// create application using router
let app = Application(
    router: router,
    configuration: .init(address: .hostname("127.0.0.1", port: 8081))
)

try await app.runService()

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
    
let sem = DispatchSemaphore(value: 0)
let url = URL(string: "http://127.0.0.1:8081")!
print("sending request to server")
let task = URLSession.shared.dataTask(with: url) { data, response, error in
    defer {sem.signal()}
    if let writeData = data {
        let arrowReader = ArrowReader()
        switch arrowReader.fromStream(writeData) {
        case .success(let result):
            let recordBatches = result.batches
            print("recordBatch: \(recordBatches.count)")
            let rb = recordBatches[0]
            print("recordBatch values: \(rb.length)")
            print("recordBatch columns: \(rb.columnCount)")
            for (idx, column) in rb.columns.enumerated() {
                print("col \(idx)")
                let array = column.array
                for idx in 0..<array.length {
                    print("data col \(idx): \(String(describing:array.asAny(idx)))")
                }
            }
        case.failure(let error):
            print("error: \(error)")
        }
    } else if let error = error {
        print("HTTP Request Failed \(error)")
    }
}

task.resume()
_ = sem.wait(timeout: .distantFuture)
print("done running http server")

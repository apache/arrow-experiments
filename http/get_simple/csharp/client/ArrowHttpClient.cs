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

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace ArrowHttpClient
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string serverUri = "http://localhost:8008/";

            DateTime startTime = DateTime.UtcNow;

            HttpClient httpClient = new HttpClient
            {
                BaseAddress = new Uri(serverUri),
            };

            using (var stream = await httpClient.GetStreamAsync(serverUri))
            using (var reader = new ArrowStreamReader(stream))
            {
                Console.WriteLine("Connected");

                List<RecordBatch> batches = new List<RecordBatch>();

                int numRows = 0;
                RecordBatch batch;
                while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
                { 
                    numRows += batch.Length;
                    batches.Add(batch);
                }
                Schema schema = reader.Schema;

                DateTime endTime = DateTime.UtcNow;
                
                Console.WriteLine($"{numRows} records received");
                Console.WriteLine($"{batches.Count} record batches received");
                Console.WriteLine($"{(endTime - startTime).TotalSeconds} seconds elapsed");
            }
        }
    }
}

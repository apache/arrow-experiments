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
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace ArrowHttpServer
{
    public class Program
    {
        static readonly Schema schema = new Schema(
            new Field[]
            {
                new Field("a", Int64Type.Default, true),
                new Field("b", Int64Type.Default, true),
                new Field("c", Int64Type.Default, true),
                new Field("d", Int64Type.Default, true),
            }, null);

        static List<RecordBatch> GenerateBatches(int totalRecords = 100000000, int batchSize = 4096)
        {
            Random random = new Random();
            IArrowArray[] columns = new IArrowArray[schema.FieldsList.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                byte[] dataBytes = new byte[batchSize * sizeof(long)];
                random.NextBytes(dataBytes);
                ArrowBuffer data = new ArrowBuffer(dataBytes);
                byte[] validityBytes = new byte[batchSize / 8];
                System.Array.Fill(validityBytes, (byte)0xff);
                ArrowBuffer validity = new ArrowBuffer(validityBytes);

                columns[i] = ArrowArrayFactory.BuildArray(new ArrayData(schema.FieldsList[i].DataType, batchSize, 0, 0, new [] { validity, data }, null));
            }
            
            List<RecordBatch> batches = new List<RecordBatch>(totalRecords);
            using (RecordBatch batch = new RecordBatch(schema, columns, batchSize))
            {
                int records = 0;
                while (records < totalRecords)
                {
                    RecordBatch newBatch = batch.Clone();
                    if (records + batchSize > totalRecords)
                    {
                        int newLength = totalRecords - records;
                        newBatch = new RecordBatch(schema, newBatch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, newLength)), newLength);
                    }

                    batches.Add(batch);
                    records += batchSize;
                }
            }

            return batches;
        }

        public static async Task Main(string[] args)
        {
            string serverUri = "http://*:8008/";

            HttpListener listener = new HttpListener();
            listener.Prefixes.Add(serverUri);
            listener.Start();

            Console.Write("Generating data... ");
            var batches = GenerateBatches();
            Console.WriteLine("done.");

            while (true)
            {
                Console.Write("Waiting... ");
                var context = await listener.GetContextAsync();
                Console.WriteLine("client connected");
                DateTime startTime = DateTime.UtcNow;

                context.Response.SendChunked = true;
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.ContentType = "application/vnd.apache.arrow.stream";

                int numRows = 0;
                using (context.Response.OutputStream)
                using (var writer = new ArrowStreamWriter(context.Response.OutputStream, schema))
                {
                    foreach (RecordBatch batch in batches)
                    {
                        await writer.WriteRecordBatchAsync(batch);
                        numRows += batch.Length;
                    }
                }
                DateTime endTime = DateTime.UtcNow;
                
                Console.WriteLine("Done");
                Console.WriteLine($"{numRows} records sent");
                Console.WriteLine($"{batches.Count} record batches sent");
                Console.WriteLine($"{(endTime - startTime).TotalSeconds} seconds elapsed");
            }
        }
    }
}

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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;

public class ArrowHttpClient {

    public static void main(String[] args) {
        String serverUrl = "http://localhost:8000";

        try {
            long startTime = System.currentTimeMillis();

            URL url = new URL(serverUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = connection.getInputStream();

                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
                List<ArrowRecordBatch> batches = new ArrayList<>();

                int num_rows = 0;
                while (reader.loadNextBatch()) { 
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    num_rows += root.getRowCount();
                    VectorUnloader unloader = new VectorUnloader(root);
                    ArrowRecordBatch batch = unloader.getRecordBatch();
                    batches.add(batch);
                }

                long endTime = System.currentTimeMillis();
                float execTime = (endTime - startTime) / 1000F; 
                
                System.out.println(reader.bytesRead() + " bytes received");
                System.out.println(num_rows + " records received");
                System.out.println(batches.size() + " record batches received");
                System.out.printf("%.2f seconds elapsed\n", execTime);

                reader.close();
            } else {
                System.err.println("Failed with response code: " + connection.getResponseCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

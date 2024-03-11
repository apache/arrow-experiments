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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ArrowHttpServer extends AbstractHandler {

    static BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    static Schema schema = new Schema(
        List.of(
            new Field("a", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("b", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("c", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("d", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

    static List<ArrowRecordBatch> batches;

    static Random random = new Random();

    public static List<ArrowRecordBatch> getPutData() {
        int totalRecords = 100000000;
        int length = 4096;

        List<ArrowRecordBatch> batches = new ArrayList<>();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            
            String[] names = schema.getFields().stream().map(Field::getName).toArray(String[]::new);
            for (String name : names) {
                byte[] randomBytes = new byte[length * 8];
                random.nextBytes(randomBytes);

                byte[] validityBytes = new byte[length / 8];
                Arrays.fill(validityBytes, (byte) 0xFF);

                BigIntVector vector = (BigIntVector) root.getVector(name);
                vector.allocateNew(length);
                vector.setValueCount(length);
                ArrowBuf dataBuffer = vector.getDataBuffer();
                dataBuffer.setBytes(0, randomBytes);

                ArrowBuf validityBuffer = vector.getValidityBuffer();
                validityBuffer.setBytes(0, validityBytes);
                root.setRowCount(length);
            }

            int records = 0;
            int lastLength;
            while (records < totalRecords) {
                if (records + length > totalRecords) {
                    lastLength = totalRecords - records;
                    try (VectorSchemaRoot slice = root.slice(0, lastLength)) {
                        VectorUnloader unloader = new VectorUnloader(slice);
                        ArrowRecordBatch arb = unloader.getRecordBatch();
                        batches.add(arb);
                    }
                    records += lastLength;
                } else {
                    VectorUnloader unloader = new VectorUnloader(root);
                    ArrowRecordBatch arb = unloader.getRecordBatch();
                    batches.add(arb);
                    records += length;
                }
            }
        }

        return batches;
    }

    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        response.setContentType("application/vnd.apache.arrow.stream");
        response.setStatus(HttpServletResponse.SC_OK);

        //// set this header to disable chunked transfer encoding:
        //response.setHeader("Connection", "close");
        
        response.flushBuffer();

        try (
            OutputStream stream = response.getOutputStream();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, stream);
        ) {
            VectorLoader loader = new VectorLoader(root);
            writer.start();
            for (ArrowRecordBatch batch : batches) {
                loader.load(batch);
                writer.writeBatch();
                stream.flush();
            }
            writer.end();
        }
        baseRequest.setHandled(true);
    }

    public static void main(String[] args) throws Exception {
        batches = getPutData();

        Server server = new Server(8008);
        server.setHandler(new ArrowHttpServer());
        server.start();
        System.out.println("Serving on localhost:8008...");
        server.join();
    }
    
}

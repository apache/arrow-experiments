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
package com.example;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.compression.ZstdCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.QuotedCSV;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ArrowHttpServer {
  static class DataGenerator {
    static final String ascii_lowercase = "abcdefghijklmnopqrstuvwxyz";
    static final String ascii_uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    final Random random;
    final BufferAllocator allocator;

    DataGenerator(Random random, BufferAllocator allocator) {
      this.random = random;
      this.allocator = allocator;
    }

    public String randomString(String alphabet, int length) {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < length; i++) {
        int index = random.nextInt(alphabet.length());
        result.append(alphabet.charAt(index));
      }
      return result.toString();
    }

    public String randomName(char initial) {
      int length = random.nextInt(5) + 3; // from 3 to 7
      return initial + randomString(ascii_lowercase, length);
    }

    public List<String> exampleTickers(int numTickers) {
      List<String> tickers = new ArrayList<>();
      while (tickers.size() < numTickers) {
        int length = random.nextInt(2) + 3; // from 3 to 4
        String randomTicker = randomString(ascii_uppercase, length);
        if (!tickers.contains(randomTicker)) {
          tickers.add(randomTicker);
        }
      }
      return tickers;
    }

    public Schema theSchema(boolean useDictionaryEncoding) {
      ArrowType.Int int32 = new ArrowType.Int(32, true);
      ArrowType.Int int64 = new ArrowType.Int(64, true);
      ArrowType utf8 = new ArrowType.Utf8();
      DictionaryEncoding dictionary = useDictionaryEncoding ? new DictionaryEncoding(0, /* isOrdered= */false, int32)
          : null;
      FieldType tickerType = new FieldType(/* nullable= */false, utf8, dictionary, null);
      FieldType priceType = FieldType.notNullable(int64);
      FieldType volumeType = FieldType.notNullable(int64);
      return new Schema(
          List.of(
              new Field("ticker", tickerType, null),
              new Field("price", priceType, null),
              new Field("volume", volumeType, null)));
    }

    public VectorSchemaRoot exampleBatch(List<String> tickers, Schema schema, int length) {
      VarCharVector ticker = new VarCharVector("ticker", allocator);
      BigIntVector price = new BigIntVector("price", allocator);
      BigIntVector volume = new BigIntVector("volume", allocator);
      ticker.allocateNew(length);
      price.allocateNew(length);
      volume.allocateNew(length);
      Text randomTicker = new Text(); // reusable UTF-8 data holder
      for (int i = 0; i < length; i++) {
        randomTicker.set(tickers.get(random.nextInt(tickers.size())));
        ticker.setSafe(i, randomTicker);
        price.set(i, (random.nextInt(1000) + 1) * 100);
        volume.set(i, random.nextInt(10000) + 1);
      }
      ticker.setValueCount(length);
      price.setValueCount(length);
      volume.setValueCount(length);
      ticker.close();
      price.close();
      volume.close();

      VectorSchemaRoot root = new VectorSchemaRoot(schema, Arrays.asList(ticker, price, volume), length);
      root.setRowCount(length);
      return root;
    }

    public List<VectorSchemaRoot> exampleBatches(List<String> tickers) {
      Schema schema = theSchema(USE_DICTIONARY_ENCODING);
      int totalRecords = 42000000;
      int batchLen = 6 * 1024;
      // All the batches sent are random slices of the larger base batch.
      VectorSchemaRoot baseBatch = exampleBatch(tickers, schema, 8 * batchLen);
      List<VectorSchemaRoot> batches = new ArrayList<>();
      int records = 0;
      while (records < totalRecords) {
        int length = Math.min(batchLen, totalRecords - records);
        int offset = random.nextInt(baseBatch.getRowCount() - length);
        VectorSchemaRoot batch = baseBatch.slice(offset, length);
        batches.add(batch);
        records += length;
      }
      return batches;
      // root.setRowCount(length);
      // VectorUnloader unloader = new VectorUnloader(root);
      // return unloader.getRecordBatch();
    }
  }

  static class Handler extends AbstractHandler {
    final static String ARROW_STREAM_FORMAT = "application/vnd.apache.arrow.stream";

    /**
     * Pick the IPC stream codec according to the Accept header.
     *
     * This is used when deciding which codec to use for compression of IPC buffer
     * streams. This is a feature of the Arrow IPC stream format and is different
     * from the HTTP content-coding used to compress the entire HTTP response.
     *
     * This is how a client may specify the IPC buffer compression codecs it
     * accepts:
     *
     * Accept: application/vnd.apache.arrow.ipc; codecs="zstd, lz4"
     *
     * @param request      The HTTP request object that may contain an Accept header
     * @param available    The list of codecs that the server can provide in the
     *                     order preferred by the server. Example: [ZSTD,
     *                     LZ4_FRAME].
     * @param defaultCodec The codec to use if the client does not specify the
     *                     ";codecs" parameter in the Accept header. Example:
     *                     Optional.of(CodecType.NO_COMPRESSION).
     * @return The codec that the server should use to compress the IPC buffer
     *         stream. Optional.empty() if the client does not accept any of the
     *         available codecs explicitly listed. ;codecs="" means no codecs are
     *         accepted. If the client does not specify the codecs parameter, then
     *         defaultCodec is returned.
     */
    static public Optional<CodecType> pickIpcCodec(
        Request request, List<CodecType> available, Optional<CodecType> defaultCodec) {
      var accept = request.getHttpFields().getField(HttpHeader.ACCEPT);

      boolean didSpecifyCodecs = false;
      ArrayList<CodecType> acceptedCodecs = new ArrayList<>();
      if (accept != null) {
        QuotedCSV mime_types = new QuotedCSV(accept.getValue());
        for (String mime_type : mime_types.getValues()) {
          HashMap<String, String> params = new HashMap<>();
          String media_range = HttpField.getValueParameters(mime_type, params);
          boolean exactMatch = media_range.equals(ARROW_STREAM_FORMAT);
          if (exactMatch || media_range.equals("*/*") || media_range.equals("application/*")) {
            if (exactMatch) {
              // Wildcards should only match when the format isn't specified
              // explicitly. So when we find an exact match, we reset the
              // accepted codecs state.
              didSpecifyCodecs = false;
              acceptedCodecs.clear();
            }
            var codecs_str = params.get("codecs");
            if (codecs_str == null) {
              continue;
            }
            didSpecifyCodecs = true;
            QuotedCSV codecs = new QuotedCSV(codecs_str);
            for (String codec : codecs.getValues()) {
              if (codec.equals("zstd")) {
                acceptedCodecs.add(CodecType.ZSTD);
              } else if (codec.equals("lz4")) {
                acceptedCodecs.add(CodecType.LZ4_FRAME);
              }
            }
            if (exactMatch) {
              // Wildcards shouldn't match after an exact match,
              // so we break the loop here.
              break;
            }
          }
        }
      }
      for (CompressionUtil.CodecType codec : available) {
        if (acceptedCodecs.contains(codec)) {
          return Optional.of(codec);
        }
      }
      return didSpecifyCodecs ? Optional.empty() : defaultCodec;
    }

    /**
     * Pick the content-coding according to the Accept-Encoding header.
     *
     * This is used when using HTTP response compression instead of IPC buffer
     * compression.
     *
     * @param request   The HTTP request object that may contain an Accept-Encoding
     *                  header
     * @param available The content-codings that the server can provide in the order
     *                  preferred by the server. Example: ["zstd", "br", "gzip"]
     * @return The content-coding that the server should use to compress the
     *         response. "identity" is returned if no acceptable content-coding is
     *         found in the list of available codings. null if the client does not
     *         accept any of the available content-codings and doesn't accept
     *         "identity" (uncompressed) either. In this case, a "406 Not
     *         Acceptable" response should be sent.
     */
    public static String pickCoding(Request request, List<String> available) {
      if (!available.contains("identity")) {
        available = new ArrayList<>(available);
        available.add("identity");
      }
      HttpField acceptEncodingField = request.getHttpFields().getField(HttpHeader.ACCEPT_ENCODING);
      if (acceptEncodingField == null) {
        return "identity";
      }
      for (String value : acceptEncodingField.getValues()) {
        if (available.contains(value)) {
          return value;
        }
      }
      // TODO: handle Accept-Encoding header
      // QuotedQualityCSV quality_csv = new QuotedQualityCSV();
      // quality_csv.addValue(acceptEncoding);
      return "identity";
    }

    static class CompressionStrategy {
      public CompressionCodec ipcCodec;
      public String httpCoding;

      /**
       * No compression at all.
       */
      public CompressionStrategy() {
        ipcCodec = NoCompressionCodec.INSTANCE;
        httpCoding = "identity";
      }

      /**
       * IPC buffer compression without HTTP compression.
       */
      public CompressionStrategy(CodecType codecType) {
        this.ipcCodec = CommonsCompressionFactory.INSTANCE.createCodec(codecType);
        this.httpCoding = "identity";
      }

      /**
       * HTTP compression without IPC buffer compression.
       */
      public CompressionStrategy(String coding) {
        this.ipcCodec = NoCompressionCodec.INSTANCE;
        this.httpCoding = coding;
      }

      /**
       * IPC buffer compression codec name to be used in HTTP headers.
       */
      public Optional<String> ipcCodecName() {
        switch (ipcCodec.getCodecType()) {
          case ZSTD:
            return Optional.of("zstd");
          case LZ4_FRAME:
            return Optional.of("lz4");
          case NO_COMPRESSION:
            return Optional.empty();
          default:
            throw new AssertionError("Unexpected codec type: " + ipcCodec.getCodecType());
        }
      }

      @Override
      public String toString() {
        return ipcCodecName().map(name -> "identity+" + name).orElse(httpCoding);
      }
    }

    /**
     * Pick the compression strategy based on the Accept and Accept-Encoding
     * headers.
     *
     * @param request            The HTTP request object that may contain Accept
     *                           and Accept-Encoding headers.
     * @param availableIpcCodecs The codecs that the server can provide for IPC
     *                           buffer compression.
     * @param availableCodings   The content-codings that the server can provide
     *                           for HTTP response compression.
     * @param defaultCompression The default compression strategy to use if the
     *                           client does explicitly choose.
     * @return The compression strategy to use or null.
     *         null means a "406 Not Acceptable" response should be sent.
     */
    static CompressionStrategy pickCompression(Request request, List<CodecType> availableIpcCodecs,
        List<String> availableCodings,
        CompressionStrategy defaultCompression) {
      // Here we decide to fallback to HTTP compression when the client doesn't
      // explicity opt-in for IPC buffer compression. So we pass defaultCodec
      // as Optional.empty() to pickIpcCodec.
      Optional<CodecType> defaultCodec = Optional.empty();
      Optional<CodecType> ipcCodecType = pickIpcCodec(request, availableIpcCodecs, defaultCodec);
      if (ipcCodecType.isEmpty()) {
        if (!request.getHttpFields().contains(HttpHeader.ACCEPT_ENCODING)) {
          return defaultCompression;
        }
        String coding = pickCoding(request, availableCodings);
        if (coding == null) {
          return null; // 406 Not Acceptable
        }
        return new CompressionStrategy(coding); // HTTP compression only
      }
      return new CompressionStrategy(ipcCodecType.get()); // IPC buffer compression
    }

    /**
     * The list of IPC buffer compression codecs that this server can provide in the
     * order preferred by the server.
     */
    final List<CodecType> availableCodecs;
    /**
     * The list of content-codings for HTTP compression that this server can provide
     * in the order preferred by the server.
     */
    final List<String> availableCodings;

    final BufferAllocator rootAllocator;
    final Schema schema;
    final List<VectorSchemaRoot> allBatches;

    Handler(BufferAllocator rootAllocator, Schema schema, List<VectorSchemaRoot> allBatches) {
      availableCodecs = Arrays.asList(CodecType.LZ4_FRAME, CodecType.ZSTD);
      availableCodings = Arrays.asList("zstd", "br", "gzip");
      this.rootAllocator = rootAllocator;
      this.schema = schema;
      this.allBatches = allBatches;
    }

    List<VectorSchemaRoot> resolveBatches() {
      return allBatches;
    }

    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
      // Create one allocator per request
      BufferAllocator allocator = rootAllocator.newChildAllocator("request", 0, Long.MAX_VALUE);

      HttpVersion version = baseRequest.getHttpVersion();
      // If client's intent cannot be derived from the headers, return
      // uncompressed data for HTTP/1.0 requests and compressed data for
      // HTTP/1.1 requests with the safest compression format choice: "gzip".
      CompressionStrategy defaultCompression = new CompressionStrategy(
          (version == HttpVersion.HTTP_1_0 || !availableCodings.contains("gzip")) ? "identity"
              : "gzip");
      CompressionStrategy compression = pickCompression(baseRequest, availableCodecs, availableCodings,
          defaultCompression);
      if (compression == null) {
        this.replyNotAcceptable(baseRequest, response);
        baseRequest.setHandled(true);
        return;
      }
      System.out.printf("Compression strategy: %s\n", compression);

      // In a real application the data would be resolved from a database or
      // another source like a file and error handling would be done here
      // before the 200 OK response starts being sent to the client.
      var batches = resolveBatches();

      response.setStatus(HttpServletResponse.SC_OK);
      //// set these headers if testing with a local browser-based client:
      // response.setHeader("Access-Control-Allow-Origin", "http://localhost:8008");
      // response.setHeader("Access-Control-Allow-Methods", "GET");
      // response.setHeader("Access-Control-Allow-Headers", "Content-Type");
      var codecName = compression.ipcCodecName();
      if (codecName.isPresent()) {
        String contentType = String.format("%s; codec=%s", ARROW_STREAM_FORMAT, codecName);
        response.setContentType(contentType);
      } else {
        response.setContentType(ARROW_STREAM_FORMAT);
      }
      // Suggest a default filename in case this response is saved by the user>
      response.setHeader("Content-Disposition", "attachment; filename=\"output.arrows\"");
      if (version == HttpVersion.HTTP_1_0) {
        response.setHeader(HttpHeader.CONNECTION.asString(), "close");
      }
      response.flushBuffer();

      // TODO: handle HTTP stream compression

      // When Jetty sees that no Content-Length is set, it will automatically
      // enable chunked transfer encoding for HTTP/1.1 responses.
      try (
          OutputStream stream = response.getOutputStream();
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, /* DictionaryProvider= */null, stream);) {
        VectorLoader loader = new VectorLoader(root);
        writer.start();
        for (VectorSchemaRoot batchRoot : batches) {
          VectorUnloader unloader = new VectorUnloader(batchRoot, true, compression.ipcCodec, true);
          ArrowRecordBatch batch = unloader.getRecordBatch();
          loader.load(batch);
          writer.writeBatch();
          stream.flush();
        }
        writer.end();
      }
      baseRequest.setHandled(true);
    }

    void replyNotAcceptable(Request baseRequest, HttpServletResponse response) throws IOException {
      response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
      response.setContentType("text/plain");
      response.setHeader("Connection", "close");
      PrintWriter writer = response.getWriter();
      writer.println("None of the available codings are accepted by this client.");
      String accept = baseRequest.getHeader(HttpHeader.ACCEPT.asString());
      if (accept != null) {
        writer.printf("`Accept` header was %s.\n", accept);
      }
      String acceptEncoding = baseRequest.getHeader(HttpHeader.ACCEPT_ENCODING.asString());
      if (acceptEncoding != null) {
        writer.printf("`Accept-Encoding` header was %s.\n", acceptEncoding);
      }
    }
  }

  static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
  static final Boolean USE_DICTIONARY_ENCODING = false;

  public static void main(String[] args) throws Exception {
    DataGenerator generator = new DataGenerator(new Random(), ROOT_ALLOCATOR);

    System.out.println("Generating example data...");
    Schema schema = generator.theSchema(USE_DICTIONARY_ENCODING);
    List<String> allTickers = generator.exampleTickers(60);
    List<VectorSchemaRoot> allBatches = generator.exampleBatches(allTickers);

    Handler handler = new Handler(ROOT_ALLOCATOR, schema, allBatches);

    Server server = new Server(8008);
    server.setHandler(handler);
    server.start();
    System.out.println("Serving on localhost:8008...");
    server.join();
  }
}

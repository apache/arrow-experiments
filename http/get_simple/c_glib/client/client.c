/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <stdlib.h>

#include <arrow-glib/arrow-glib.h>
#include <libsoup/soup.h>

int
main(int argc, char **argv)
{
  int exit_code = EXIT_FAILURE;

  SoupSession *session = soup_session_new();
  SoupMessage *message = soup_message_new(SOUP_METHOD_GET,
                                          "http://localhost:8008");
  /* Disable keep-alive explicitly. (libsoup uses keep-alive by
   * default.)
   *
   * In general, keep-alive will improve performance when we sends
   * many GET requests to the same server. But in this case, we send
   * only one GET request. So we don't need keep-alive here.
   */
  SoupMessageHeaders *headers = soup_message_get_request_headers(message);
  soup_message_headers_append(headers, "connection", "close");

  GTimer *timer = g_timer_new();

  GError *error = NULL;
  GInputStream *input = soup_session_send(session, message, NULL, &error);
  if (error) {
    g_printerr("Failed to download: %s\n", error->message);
    g_error_free(error);
    goto exit;
  }

  GArrowGIOInputStream *arrow_input = garrow_gio_input_stream_new(input);
  GArrowRecordBatchStreamReader *reader =
    garrow_record_batch_stream_reader_new(GARROW_INPUT_STREAM(arrow_input),
                                          &error);
  if (error) {
    g_printerr("Failed to create reader: %s\n", error->message);
    g_error_free(error);
    g_object_unref(arrow_input);
    goto exit;
  }

  GArrowTable *table =
    garrow_record_batch_reader_read_all(GARROW_RECORD_BATCH_READER(reader),
                                        &error);
  if (error) {
    g_printerr("Failed to read record batches: %s\n", error->message);
    g_error_free(error);
    g_object_unref(reader);
    g_object_unref(arrow_input);
    goto exit;
  }
  GArrowChunkedArray *chunked_array = garrow_table_get_column_data(table, 0);
  guint n_received_record_batches =
    garrow_chunked_array_get_n_chunks(chunked_array);
  g_object_unref(chunked_array);
  g_object_unref(table);
  g_object_unref(reader);
  g_object_unref(arrow_input);

  g_timer_stop(timer);

  g_print("%u record batches received\n", n_received_record_batches);

  g_print("%.2f seconds elapsed\n", g_timer_elapsed(timer, NULL));

  exit_code = EXIT_SUCCESS;

exit:
  g_object_unref(input);
  g_timer_destroy(timer);
  g_object_unref(message);
  g_object_unref(session);

  return exit_code;
}

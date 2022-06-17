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

package org.apache.druid.tests.indexer;

import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = TestNGGroup.INPUT_SOURCE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITHttpInputSourceTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_http_inputsource_task_template.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_http_inputsource_queries.json";

  private static final String WIKIPEDIA_DATASET_RESOURCE = "/data/batch_index/json/wikipedia_index_data1.json";

  public void doTest() throws IOException
  {
    final String indexDatasource = "wikipedia_http_inputsource_test_" + UUID.randomUUID();
    HttpServer server = null;
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(0);
      int port = serverSocket.getLocalPort();
      // closing port so that the httpserver can use. Can cause race conditions.
      serverSocket.close();
      // workers should be able to connect to this port.
      server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 10);
      server.createContext(
          "/wikipedia.json",
          (httpExchange) -> {
            String payload = getResourceAsString(WIKIPEDIA_DATASET_RESOURCE);
            byte[] outputBytes = payload.getBytes(StandardCharsets.UTF_8);
            httpExchange.sendResponseHeaders(200, outputBytes.length);
            OutputStream os = httpExchange.getResponseBody();
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_LENGTH, String.valueOf(outputBytes.length));
            httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_RANGE, "bytes 0");
            os.write(outputBytes);
            os.close();
          }
      );
      server.start();
      String taskPayload = StringUtils.replace(
          getResourceAsString(INDEX_TASK),
          "%%url%%",
          InetAddress.getLocalHost().getHostAddress() + ":" + server.getAddress().getPort()
      );
      try (final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix())) {
        doIndexTask(
            indexDatasource,
            Function.identity(),
            INDEX_QUERIES_RESOURCE,
            false,
            true,
            true,
            new Pair<>(null, null),
            taskPayload
        );
      }
    }
    finally {
      if (server != null) {
        server.stop(0);
      }
      if (serverSocket != null) {
        serverSocket.close();
      }
    }
  }
}

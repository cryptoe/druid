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

package org.apache.druid.server.metrics;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class QueryCountStatsMonitorTest
{
  private QueryCountStatsProvider queryCountStatsProvider;
  private BlockingPool<ByteBuffer> mergeBufferPool;
  private MonitorsConfig monitorsConfig;
  private ExecutorService executorService;

  @Before
  public void setUp()
  {
    monitorsConfig = new MonitorsConfig(
        new ArrayList<>(
            Arrays.asList(
                GroupByStatsMonitor.class.getName(),
                QueryCountStatsMonitor.class.getName()
            )
        )
    );

    queryCountStatsProvider = new QueryCountStatsProvider()
    {
      private long successEmitCount = 0;
      private long failedEmitCount = 0;
      private long interruptedEmitCount = 0;
      private long timedOutEmitCount = 0;

      @Override
      public long getSuccessfulQueryCount()
      {
        successEmitCount += 1;
        return successEmitCount;
      }

      @Override
      public long getFailedQueryCount()
      {
        failedEmitCount += 2;
        return failedEmitCount;
      }

      @Override
      public long getInterruptedQueryCount()
      {
        interruptedEmitCount += 3;
        return interruptedEmitCount;
      }

      @Override
      public long getTimedOutQueryCount()
      {
        timedOutEmitCount += 4;
        return timedOutEmitCount;
      }
    };

    mergeBufferPool = new DefaultBlockingPool(() -> ByteBuffer.allocate(1024), 5);
    executorService = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown()
  {
    executorService.shutdown();
  }

  @Test
  public void testMonitor()
  {
    final QueryCountStatsMonitor monitor =
        new QueryCountStatsMonitor(queryCountStatsProvider, monitorsConfig, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);
    Map<String, Long> resultMap = emitter.getEvents()
                                         .stream()
                                         .collect(Collectors.toMap(
                                             event -> (String) event.toMap().get("metric"),
                                             event -> (Long) event.toMap().get("value")
                                         ));
    Assert.assertEquals(5, resultMap.size());
    Assert.assertEquals(1L, (long) resultMap.get("query/success/count"));
    Assert.assertEquals(2L, (long) resultMap.get("query/failed/count"));
    Assert.assertEquals(3L, (long) resultMap.get("query/interrupted/count"));
    Assert.assertEquals(4L, (long) resultMap.get("query/timeout/count"));
    Assert.assertEquals(10L, (long) resultMap.get("query/count"));
  }

  @Test
  public void testMonitor_emitPendingRequests()
  {
    monitorsConfig = new MonitorsConfig(Collections.singletonList(QueryCountStatsMonitor.class.getName()));

    final QueryCountStatsMonitor monitor =
        new QueryCountStatsMonitor(queryCountStatsProvider, monitorsConfig, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);
    Map<String, Long> resultMap = emitter.getEvents()
                                         .stream()
                                         .collect(Collectors.toMap(
                                             event -> (String) event.toMap().get("metric"),
                                             event -> (Long) event.toMap().get("value")
                                         ));
    Assert.assertEquals(6, resultMap.size());
    Assert.assertEquals(0, (long) resultMap.get("mergeBuffer/pendingRequests"));
    Assert.assertEquals(1L, (long) resultMap.get("query/success/count"));
    Assert.assertEquals(2L, (long) resultMap.get("query/failed/count"));
    Assert.assertEquals(3L, (long) resultMap.get("query/interrupted/count"));
    Assert.assertEquals(4L, (long) resultMap.get("query/timeout/count"));
    Assert.assertEquals(10L, (long) resultMap.get("query/count"));
  }
}

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

package org.apache.druid.server.coordination;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.BootstrapSegmentsResponse;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Set;

public class TestCoordinatorClient extends NoopCoordinatorClient
{
  private final Set<DataSegment> bootstrapSegments;
  private final CoordinatorDynamicConfig config;

  public TestCoordinatorClient()
  {
    this(new HashSet<>(), CoordinatorDynamicConfig.builder().build());
  }

  public TestCoordinatorClient(final Set<DataSegment> bootstrapSegments)
  {
    this(bootstrapSegments, CoordinatorDynamicConfig.builder().build());
  }

  public TestCoordinatorClient(final Set<DataSegment> bootstrapSegments, final CoordinatorDynamicConfig config)
  {
    this.bootstrapSegments = bootstrapSegments;
    this.config = config;
  }

  @Override
  public ListenableFuture<BootstrapSegmentsResponse> fetchBootstrapSegments()
  {
    return Futures.immediateFuture(
        new BootstrapSegmentsResponse(CloseableIterators.withEmptyBaggage(bootstrapSegments.iterator()))
    );
  }

  @Override
  public ListenableFuture<CoordinatorDynamicConfig> getCoordinatorDynamicConfig()
  {
    return Futures.immediateFuture(config);
  }
}

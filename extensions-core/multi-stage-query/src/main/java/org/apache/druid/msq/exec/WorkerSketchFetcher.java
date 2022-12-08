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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.function.TriConsumer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Queues up fetching sketches from workers and progressively generates partitions boundaries.
 */
public class WorkerSketchFetcher
{
  private static final Logger log = new Logger(WorkerSketchFetcher.class);
  private static final int DEFAULT_THREAD_COUNT = 4;
  // If the combined size of worker sketches is more than this threshold, SEQUENTIAL merging mode is used.
  public static final long WORKER_THRESHOLD = 100;

  private final WorkerClient workerClient;
  private final ExecutorService executorService;
  private final MSQWorkerTaskLauncher workerTaskLauncher;

  private final boolean retryEnabled;

  AtomicReference<Throwable> isError = new AtomicReference<>();

  public WorkerSketchFetcher(
      WorkerClient workerClient,
      MSQWorkerTaskLauncher workerTaskLauncher,
      boolean retryEnabled
  )
  {
    this.workerClient = workerClient;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
    this.workerTaskLauncher = workerTaskLauncher;
    this.retryEnabled = retryEnabled;
  }

  /**
   * Submits a request to fetch and generate partitions for the given worker statistics and returns a future for it. It
   * decides based on the statistics if it should fetch sketches one by one or together.
   * The future can successfully return a null signaling that partition statistics from all the workers have not been fetched yet.
   *
   */


  /**
   * Fetches the full {@link ClusterByStatisticsCollector} from all workers and generates partition boundaries from them.
   * This is faster than fetching them timechunk by timechunk but the collector will be downsampled till it can fit
   * on the controller, resulting in less accurate partition boundries.
   */
  public void inMemoryFullSketchMerging(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      StageId stageId,
      Set<String> taskIds,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation
  )
  {

    for (String taskId : taskIds) {
      try {
        int workerNumber = MSQTasks.workerFromTaskId(taskId);
        executorService.submit(() -> {
          fetchStatsFromWorker(
              kernelActions,
              () -> workerClient.fetchClusterByStatisticsSnapshot(
                  taskId,
                  stageId.getQueryId(),
                  stageId.getStageNumber()
              ),
              taskId,
              (kernel, snapshot) -> kernel.mergeClusterByStatisticsCollectorForAllTimeChunks(
                  stageId,
                  workerNumber,
                  snapshot
              ),
              retryOperation
          );
        });
      }
      catch (RejectedExecutionException rejectedExecutionException) {
        if (isError.get() == null) {
          throw rejectedExecutionException;
        } else {
          // throw worker error exception
          throw new ISE("Unable to fetch partitions ", isError.get());
        }
      }
    }
  }

  private void fetchStatsFromWorker(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      Supplier<ListenableFuture<ClusterByStatisticsSnapshot>> fetchStatsSupplier,
      String taskId,
      BiConsumer<ControllerQueryKernel, ClusterByStatisticsSnapshot> successKernelOperation,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation
  )
  {
    if (isError.get() != null) {
      executorService.shutdownNow();
      return;
    }
    try {
      workerTaskLauncher.waitUntilWorkersReady(ImmutableSet.of(MSQTasks.workerFromTaskId(taskId)));
    }
    catch (InterruptedException interruptedException) {
      isError.compareAndSet(null, interruptedException);
      executorService.shutdownNow();
      return;
    }

    ListenableFuture<ClusterByStatisticsSnapshot> fetchFuture = fetchStatsSupplier.get();

    SettableFuture<Boolean> kernelActionFuture = SettableFuture.create();

    Futures.addCallback(fetchFuture, new FutureCallback<ClusterByStatisticsSnapshot>()
    {
      @Override
      public void onSuccess(@Nullable ClusterByStatisticsSnapshot result)
      {
        kernelActions.accept((queryKernel) -> {
          successKernelOperation.accept(queryKernel, result);
          // we do not want to have too many key collector sketches in the event queue as that cause memory issues
          // blocking the executor service thread until the kernel operation is finished.
          // so we would have utmost DEFAULT_THREAD_COUNT number of sketches in the queue.
          kernelActionFuture.set(true);
        });

      }

      @Override
      public void onFailure(Throwable t)
      {
        if (retryEnabled) {
          //add to retry queue
          kernelActions.accept((kernel) -> {
            retryOperation.accept(kernel, MSQTasks.workerFromTaskId(taskId), new WorkerRpcFailedFault(taskId));
            kernelActionFuture.set(false);
          });
          kernelActionFuture.set(false);

        } else {
          if (isError.compareAndSet(null, t)) {
            log.error(t, "Failed while fetching stats from task[%s]", taskId);
          }
          executorService.shutdownNow();
          kernelActionFuture.setException(t);
        }
      }
    });

    FutureUtils.getUnchecked(kernelActionFuture, true);
  }

  /**
   * Fetches cluster statistics from all workers and generates partition boundaries from them one time chunk at a time.
   * This takes longer due to the overhead of fetching sketches, however, this prevents any loss in accuracy from
   * down sampling on the controller.
   */
  public void sequentialTimeChunkMerging(
      Consumer<Consumer<ControllerQueryKernel>> kernelActions,
      CompleteKeyStatisticsInformation completeKeyStatisticsInformation,
      StageId stageId,
      Set<String> tasks,
      TriConsumer<ControllerQueryKernel, Integer, MSQFault> retryOperation
  )
  {
    if (!completeKeyStatisticsInformation.isComplete()) {
      throw new ISE("All worker partial key information not received for stage[%d]", stageId.getStageNumber());
    }
    Set<Integer> workers = tasks.stream().map(MSQTasks::workerFromTaskId).collect(Collectors.toSet());
    completeKeyStatisticsInformation.getTimeSegmentVsWorkerMap().forEach((timeChunk, wks) -> {

      for (String taskId : tasks) {
        int workerNumber = MSQTasks.workerFromTaskId(taskId);
        if (workers.contains(workerNumber)) {
          executorService.submit(() -> {
            fetchStatsFromWorker(
                kernelActions,
                () -> workerClient.fetchClusterByStatisticsSnapshotForTimeChunk(
                    taskId,
                    stageId.getQueryId(),
                    stageId.getStageNumber(),
                    timeChunk
                ),
                taskId,
                (kernel, snapshot) -> kernel.mergeClusterByStatisticsCollectorForTimeChunk(
                    stageId,
                    workerNumber,
                    timeChunk,
                    snapshot
                ),
                retryOperation
            );

          });
        }
      }
    });
  }
}

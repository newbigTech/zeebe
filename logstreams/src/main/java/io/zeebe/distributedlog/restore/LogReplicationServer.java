/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.restore;

import java.util.concurrent.CompletableFuture;

public interface LogReplicationServer extends AutoCloseable {

  /**
   * Start serving {@link LogReplicationRequest} requests to the given handler.
   *
   * @param server handler to handle incoming requests
   * @return a future which will complete once the server is ready to accept requests
   */
  CompletableFuture<Void> serve(Handler server);

  @Override
  void close();

  @FunctionalInterface
  interface Handler {

    /**
     * Each request will contain a range of event positions, and expect the returned events to have
     * positions strictly contained within that range. Furthermore, the first event serialized in
     * the response must have a position equal to the request {@link
     * LogReplicationRequest#getFromPosition()}. If no event is found locally with that position,
     * the returned response must be invalid.
     *
     * <p>Additionally, events must be serialized in the order they were read on the local log, and
     * the last event position must be less than or equal to the {@link
     * LogReplicationRequest#getToPosition()}. If that position is less than the requested {@code
     * toPosition}, but there are more events locally with positions less than the {@code
     * toPosition}, then the response should indicate that and {@link
     * LogReplicationResponse#hasMoreAvailable()} should be true.
     *
     * @param request the request to server
     * @return response to return to the sender
     */
    LogReplicationResponse onReplicationRequest(LogReplicationRequest request);
  }
}

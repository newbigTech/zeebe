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
package io.zeebe.distributedlog.restore.impl;

import io.zeebe.distributedlog.restore.LogReplicationRequest;
import io.zeebe.distributedlog.restore.LogReplicationResponse;
import io.zeebe.distributedlog.restore.LogReplicationServer;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.util.ZbLogger;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public class DefaultLogReplicationServerHandler implements LogReplicationServer.Handler {
  private static final int DEFAULT_READ_BUFFER_SIZE = 64 * 1024 * 1024;

  private final LogStreamReader reader;
  private final MutableDirectBuffer readerBuffer;
  private final Logger logger;

  public DefaultLogReplicationServerHandler(LogStream logStream) {
    this(logStream, DEFAULT_READ_BUFFER_SIZE);
  }

  public DefaultLogReplicationServerHandler(LogStream logStream, int bufferSize) {
    this.reader = new BufferedLogStreamReader(logStream);
    this.readerBuffer = new UnsafeBuffer(ByteBuffer.allocate(bufferSize));
    this.logger = new ZbLogger(String.format("log.replication.server.%s", logStream.getLogName()));
  }

  @Override
  public final LogReplicationResponse onReplicationRequest(LogReplicationRequest request) {
    final DefaultLogReplicationResponse response = new DefaultLogReplicationResponse();

    if (seek(request.getFromPosition())) {
      long lastReadPosition = reader.getPosition();
      int offset = 0;

      while (reader.hasNext() && offset < readerBuffer.capacity()) {
        final LoggedEvent event = reader.next();
        if (event.getPosition() <= request.getToPosition()) {
          event.write(readerBuffer, offset);
          offset += event.getLength();
          lastReadPosition = event.getPosition();
        }
      }

      response.setToPosition(lastReadPosition);
      response.setMoreAvailable(lastReadPosition < request.getToPosition() && reader.hasNext());
      response.setSerializedEvents(readerBuffer, 0, offset);
    } else {
      logger.debug(
          "Ignoring log replication request {} - {}, no events found with position {}",
          request.getFromPosition(),
          request.getToPosition(),
          request.getFromPosition());
    }

    return response;
  }

  private boolean seek(long position) {
    if (position == -1) {
      reader.seekToFirstEvent();
      return true;
    }

    return reader.seek(position);
  }
}

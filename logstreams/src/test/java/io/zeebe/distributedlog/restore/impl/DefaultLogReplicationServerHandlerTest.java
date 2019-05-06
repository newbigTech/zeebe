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

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.distributedlog.restore.LogReplicationRequest;
import io.zeebe.distributedlog.restore.LogReplicationResponse;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.util.LogStreamReaderRule;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.logstreams.util.LogStreamWriterRule;
import io.zeebe.test.util.MsgPackUtil;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class DefaultLogReplicationServerHandlerTest {
  private final TemporaryFolder logFolder = new TemporaryFolder();
  private final LogStreamRule logStreamRule = new LogStreamRule(logFolder);
  private final LogStreamWriterRule logStreamWriterRule = new LogStreamWriterRule(logStreamRule);
  private final LogStreamReaderRule logStreamReaderRule = new LogStreamReaderRule(logStreamRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(logFolder)
          .around(logStreamRule)
          .around(logStreamWriterRule)
          .around(logStreamReaderRule);

  @Test
  public void shouldReplicateRequestedEvents() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int requestedCount = 10;
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(loggedEvents);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(
            loggedEvents.get(0).getPosition(), loggedEvents.get(requestedCount - 1).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(response.getToPosition())
        .isEqualTo(loggedEvents.get(requestedCount - 1).getPosition());
    assertThat(response.hasMoreAvailable()).isFalse();
    assertThat(response.getSerializedEvents()).isEqualTo(serialize(loggedEvents));
    assertThat(response.isValid()).isTrue();
  }

  @Test
  public void shouldOnlyReplicateAsMuchAsFitsTheBuffer() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int requestedCount = 5;
    final List<LoggedEvent> expected = loggedEvents.subList(0, requestedCount);
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(expected);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(
            loggedEvents.get(0).getPosition(), loggedEvents.get(9).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(response.getToPosition())
        .isEqualTo(loggedEvents.get(requestedCount - 1).getPosition());
    assertThat(response.hasMoreAvailable()).isTrue();
    assertThat(response.getSerializedEvents()).isEqualTo(serialize(expected));
    assertThat(response.isValid()).isTrue();
  }

  @Test
  public void shouldReplicateUpToRequestedPosition() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int requestedCount = 8;
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(loggedEvents);
    final List<LoggedEvent> expected = loggedEvents.subList(0, requestedCount);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(
            expected.get(0).getPosition(), loggedEvents.get(requestedCount - 1).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(response.getToPosition())
        .isEqualTo(loggedEvents.get(requestedCount - 1).getPosition());
    assertThat(response.hasMoreAvailable()).isFalse();
    assertThat(response.getSerializedEvents()).isEqualTo(serialize(expected));
    assertThat(response.isValid()).isTrue();
  }

  @Test
  public void shouldReplicateFromRequestedPosition() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int requestedCount = 5;
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(loggedEvents);
    final List<LoggedEvent> expected = loggedEvents.subList(requestedCount, 10);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(
            loggedEvents.get(requestedCount).getPosition(), loggedEvents.get(9).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(deserialize(response.getSerializedEvents()).get(0).getPosition())
        .isEqualTo(expected.get(0).getPosition());
    assertThat(response.getToPosition()).isEqualTo(loggedEvents.get(9).getPosition());
    assertThat(response.hasMoreAvailable()).isFalse();
    assertThat(response.getSerializedEvents()).isEqualTo(serialize(expected));
    assertThat(response.isValid()).isTrue();
  }

  @Test
  public void shouldHaveNoEventsIfFromPositionIsNotFound() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(loggedEvents);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(
            loggedEvents.get(9).getPosition() + 1, loggedEvents.get(9).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(response.hasMoreAvailable()).isFalse();
    assertThat(response.isValid()).isFalse();
    assertThat(response.getSerializedEvents()).isNullOrEmpty();
  }

  @Test
  public void shouldReplicateFromFirstPositionIfFromIsNegative() {
    // given
    final List<LoggedEvent> loggedEvents = writeEvents(10);
    final int requestedCount = 10;
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(loggedEvents);
    final LogReplicationRequest request =
        new DefaultLogReplicationRequest(-1, loggedEvents.get(requestedCount - 1).getPosition());
    final DefaultLogReplicationServerHandler handler =
        new DefaultLogReplicationServerHandler(logStreamRule.getLogStream(), bufferSize);

    // when
    final LogReplicationResponse response = handler.onReplicationRequest(request);

    // then
    assertThat(response.getToPosition())
        .isEqualTo(loggedEvents.get(requestedCount - 1).getPosition());
    assertThat(response.hasMoreAvailable()).isFalse();
    assertThat(response.getSerializedEvents()).isEqualTo(serialize(loggedEvents));
    assertThat(response.isValid()).isTrue();
  }

  private List<LoggedEvent> deserialize(byte[] serialized) {
    final DirectBuffer buffer = new UnsafeBuffer(serialized);
    final List<LoggedEvent> events = new ArrayList<>();
    int offset = 0;

    do {
      final LoggedEventImpl event = new LoggedEventImpl();
      event.wrap(buffer, offset);
      events.add(event);
      offset += event.getFragmentLength();
    } while (offset < buffer.capacity());

    return events;
  }

  private List<LoggedEvent> writeEvents(int count) {
    final DirectBuffer event = MsgPackUtil.asMsgPack("{}");
    logStreamWriterRule.writeEvents(count, event);
    return logStreamReaderRule.readEvents();
  }

  private byte[] serialize(List<LoggedEvent> events) {
    final int bufferSize = calculateMinimumHandlerBufferSizeForEvents(events);
    final byte[] buffer = new byte[bufferSize];
    final MutableDirectBuffer copyBuffer = new UnsafeBuffer(buffer);

    for (int i = 0, offset = 0; i < events.size(); offset += events.get(i).getLength(), i++) {
      events.get(i).write(copyBuffer, offset);
    }

    return buffer;
  }

  private int calculateMinimumHandlerBufferSizeForEvents(List<LoggedEvent> events) {
    return events.stream().mapToInt(LoggedEvent::getLength).sum();
  }
}

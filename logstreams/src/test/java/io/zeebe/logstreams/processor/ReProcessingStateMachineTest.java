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
package io.zeebe.logstreams.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.zeebe.db.DbContext;
import io.zeebe.db.TransactionOperation;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import java.util.concurrent.CountDownLatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ReProcessingStateMachineTest {

  @Rule public ControlledActorSchedulerRule actorSchedulerRule = new ControlledActorSchedulerRule();

  private ReProcessingStateMachine reProcessingStateMachine;

  @Mock private StreamProcessor streamProcessor;
  @Mock private LogStreamReader logStreamReader;
  @Mock private DbContext dbContext;

  private ZeebeDbTransaction zeebeDbTransaction;
  private ActorControl actor;
  private EventProcessor eventProcessor;
  private LoggedEvent loggedEvent;

  @Before
  public void setup() {
    initMocks(this);

    final ControllableActor controllableActor = new ControllableActor();
    actor = controllableActor.getActor();

    when(logStreamReader.hasNext()).thenReturn(true, true, false, true, false);
    loggedEvent = mock(LoggedEvent.class);
    when(loggedEvent.getSourceEventPosition()).thenReturn(1L);
    when(logStreamReader.next()).thenReturn(loggedEvent);

    zeebeDbTransaction = spy(new Transaction());
    when(dbContext.getCurrentTransaction()).thenReturn(zeebeDbTransaction);

    eventProcessor = mock(EventProcessor.class);
    when(streamProcessor.onEvent(any())).thenReturn(eventProcessor);
    when(streamProcessor.getFailedPosition(any())).thenReturn(-1L);

    final StreamProcessorContext streamProcessorContext = new StreamProcessorContext();
    streamProcessorContext.setActorControl(actor);
    streamProcessorContext.setLogStreamReader(logStreamReader);
    streamProcessorContext.setName("testProcessor");

    reProcessingStateMachine =
        ReProcessingStateMachine.builder()
            .setStreamProcessorContext(streamProcessorContext)
            .setStreamProcessor(streamProcessor)
            .setDbContext(dbContext)
            .setAbortCondition(() -> false)
            .build();

    actorSchedulerRule.submitActor(controllableActor);
  }

  @Test
  public void shouldRunLifecycle() throws Exception {
    // given
    final CountDownLatch latch = new CountDownLatch(1);

    // when
    actor.call(
        () -> {
          final ActorFuture<Void> recoverFuture = reProcessingStateMachine.startRecover(0L);
          actor.runOnCompletion(recoverFuture, (v, t) -> latch.countDown());
        });
    actorSchedulerRule.workUntilDone();

    // then
    latch.await();
    final InOrder inOrder =
        Mockito.inOrder(streamProcessor, eventProcessor, dbContext, zeebeDbTransaction);
    inOrder.verify(streamProcessor, times(1)).onEvent(any());

    // process
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
    inOrder.verify(eventProcessor, times(1)).processEvent();

    // update state
    inOrder.verify(zeebeDbTransaction, times(1)).commit();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRunLifecycleWithFailedEvent() throws Exception {
    // given
    final CountDownLatch latch = new CountDownLatch(1);
    when(streamProcessor.getFailedPosition(any())).thenReturn(1L);
    when(loggedEvent.getPosition()).thenReturn(1L);

    // when
    actor.call(
        () -> {
          final ActorFuture<Void> recoverFuture = reProcessingStateMachine.startRecover(0L);
          actor.runOnCompletion(recoverFuture, (v, t) -> latch.countDown());
        });
    actorSchedulerRule.workUntilDone();

    // then
    latch.await();
    final InOrder inOrder =
        Mockito.inOrder(streamProcessor, eventProcessor, dbContext, zeebeDbTransaction);
    inOrder.verify(streamProcessor, times(1)).onEvent(any());

    // event on error
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
    inOrder.verify(eventProcessor, times(1)).onError(any());

    // update state
    inOrder.verify(zeebeDbTransaction, times(1)).commit();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRetryProcessingUntilSuccess() throws Exception {
    // given
    doThrow(new RuntimeException("expected"))
        .doThrow(new RuntimeException("expected"))
        .doCallRealMethod()
        .when(zeebeDbTransaction)
        .run(any());
    final CountDownLatch latch = new CountDownLatch(1);

    // when
    actor.call(
        () -> {
          final ActorFuture<Void> recoverFuture = reProcessingStateMachine.startRecover(0L);
          actor.runOnCompletion(recoverFuture, (v, t) -> latch.countDown());
        });
    actorSchedulerRule.workUntilDone();

    // then
    latch.await();
    final InOrder inOrder =
        Mockito.inOrder(streamProcessor, eventProcessor, dbContext, zeebeDbTransaction);
    inOrder.verify(streamProcessor, times(1)).onEvent(any());
    // process
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());

    // first - retry
    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());

    // second - retry
    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
    inOrder.verify(eventProcessor, times(1)).processEvent();

    // update state
    inOrder.verify(zeebeDbTransaction, times(1)).commit();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRetryUpdateStateUntilSuccess() throws Exception {
    // given
    doThrow(new RuntimeException("expected"))
        .doThrow(new RuntimeException("expected"))
        .doNothing()
        .when(zeebeDbTransaction)
        .commit();
    final CountDownLatch latch = new CountDownLatch(1);

    // when
    actor.call(
        () -> {
          final ActorFuture<Void> recoverFuture = reProcessingStateMachine.startRecover(-1L);
          actor.runOnCompletion(recoverFuture, (v, t) -> latch.countDown());
        });
    actorSchedulerRule.workUntilDone();

    // then
    latch.await();
    final InOrder inOrder =
        Mockito.inOrder(streamProcessor, eventProcessor, dbContext, zeebeDbTransaction);
    inOrder.verify(streamProcessor, times(1)).getFailedPosition(any());
    inOrder.verify(streamProcessor, times(1)).onEvent(any());

    // process
    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
    inOrder.verify(eventProcessor, times(1)).processEvent();

    // update state with 2 retries
    inOrder.verify(zeebeDbTransaction, times(3)).commit();

    inOrder.verifyNoMoreInteractions();
  }

  private class ControllableActor extends Actor {

    public ActorControl getActor() {
      return actor;
    }
  }

  private class Transaction implements ZeebeDbTransaction {

    @Override
    public void run(TransactionOperation operations) {
      try {
        operations.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commit() {}

    @Override
    public void rollback() {}
  }
}

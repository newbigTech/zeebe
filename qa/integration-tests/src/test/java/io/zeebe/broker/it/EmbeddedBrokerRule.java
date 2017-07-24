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
package io.zeebe.broker.it;

import static io.zeebe.broker.task.TaskQueueServiceNames.taskQueueInstanceStreamProcessorServiceName;
import static io.zeebe.logstreams.log.LogStream.DEFAULT_LOG_NAME;

import java.io.InputStream;
import java.util.concurrent.*;
import java.util.function.Supplier;

import io.zeebe.broker.Broker;
import io.zeebe.broker.event.TopicSubscriptionServiceNames;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.*;
import io.zeebe.util.allocation.DirectBufferAllocator;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;

public class EmbeddedBrokerRule extends ExternalResource
{
    protected static final Logger LOG = TestLoggers.TEST_LOGGER;

    protected Broker broker;

    protected Supplier<InputStream> configSupplier;

    public EmbeddedBrokerRule()
    {
        this(() -> null);
    }

    public EmbeddedBrokerRule(Supplier<InputStream> configSupplier)
    {
        this.configSupplier = configSupplier;
    }

    @Override
    protected void before() throws Throwable
    {
        startBroker();
    }

    @Override
    protected void after()
    {
        stopBroker();
        broker = null;
        System.gc();

        final long allocatedMemoryInKb = DirectBufferAllocator.getAllocatedMemoryInKb();
        if (allocatedMemoryInKb > 0)
        {
            LOG.warn("There are still allocated direct buffers of a total size of {}kB.", allocatedMemoryInKb);
        }
    }

    public void restartBroker()
    {
        stopBroker();
        startBroker();
    }

    public void stopBroker()
    {
        broker.close();
    }

    public void startBroker()
    {
        broker = new Broker(configSupplier.get());

        final ServiceContainer serviceContainer = broker.getBrokerContext().getServiceContainer();

        try
        {
            // Hack: block until default task queue log has been installed
            // How to make it better: https://github.com/camunda-tngp/zeebe/issues/196
            serviceContainer.createService(TestService.NAME, new TestService())
                .dependency(TopicSubscriptionServiceNames.subscriptionManagementServiceName(DEFAULT_LOG_NAME))
                .dependency(TransportServiceNames.serverTransport(TransportServiceNames.CLIENT_API_SERVER_NAME))
                .dependency(taskQueueInstanceStreamProcessorServiceName(DEFAULT_LOG_NAME))
                .install()
                .get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            stopBroker();
            throw new RuntimeException("Default task queue log not installed into the container withing 5 seconds.");
        }
    }

    static class TestService implements Service<TestService>
    {

        static final ServiceName<TestService> NAME = ServiceName.newServiceName("testService", TestService.class);

        @Override
        public void start(ServiceStartContext startContext)
        {

        }

        @Override
        public void stop(ServiceStopContext stopContext)
        {

        }

        @Override
        public TestService get()
        {
            return this;
        }

    }

}
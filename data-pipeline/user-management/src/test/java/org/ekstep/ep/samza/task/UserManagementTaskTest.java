package org.ekstep.ep.samza.task;


import com.google.gson.Gson;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.ekstep.ep.samza.fixtures.EventFixture;
import org.ekstep.ep.samza.model.Event;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

public class UserManagementTaskTest {
    private static final String SUCCESS_TOPIC = "SUCCESS_TOPIC";
    private static final String FAILURE_TOPIC = "FAILURE_TOPIC";
    private static final String DEFAULT_CHANNEL = "in.ekstep.test";
    private Config configMock;
    private TaskContext contextMock;
    private MessageCollector collectorMock;
    private IncomingMessageEnvelope envelopMock;
    private TaskCoordinator coordinatorMock;
    private MetricsRegistry metricsRegistryMock;
    private Counter counterMock;

    @Before
    public void setup() {

        configMock = Mockito.mock(Config.class);
        contextMock = Mockito.mock(TaskContext.class);
        collectorMock = mock(MessageCollector.class);
        envelopMock = mock(IncomingMessageEnvelope.class);
        coordinatorMock = mock(TaskCoordinator.class);
        metricsRegistryMock = mock(MetricsRegistry.class);
        counterMock = mock(Counter.class);

        when(configMock.get("output.success.topic.name", "sandbox.learners")).thenReturn(SUCCESS_TOPIC);
        when(configMock.get("output.failed.topic.name", "sandbox.learners.fail")).thenReturn(FAILURE_TOPIC);
        when(configMock.get("default.channel", "in.ekstep")).thenReturn(DEFAULT_CHANNEL);
        when(configMock.get("db.url")).thenReturn("jdbc:mysql://localhost:3306/ecosystem");
        when(configMock.get("db.userName")).thenReturn("jenkins");
        when(configMock.get("db.password")).thenReturn("password");
        when(configMock.get("db.connectionTimeoutMs")).thenReturn("10000");
        when(contextMock.getMetricsRegistry()).thenReturn(metricsRegistryMock);
        when(metricsRegistryMock.newCounter("org.ekstep.ep.samza.task.UserManagementTask", "message-count")).thenReturn(counterMock);
    }

    @Test
    public void shouldCreateNewLearnerEntryToLearnerTable() throws Exception {

        Event event = new Event(new EventFixture().CREATE_USER_EVENT);

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.processEvent(event, collectorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), SUCCESS_TOPIC)));

    }

    @Test
    public void shouldCreateNewProfileEntryToProfileTable() throws Exception {

        Event event = new Event(new EventFixture().CREATE_PROFILE_EVENT);

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.processEvent(event, collectorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), SUCCESS_TOPIC)));
    }

    @Test
    public void shouldTestProcessMethodAndCreateNewLearnerEntry() throws Exception {
        Gson gson = new Gson();
        Event event = new Event(new EventFixture().CREATE_USER_EVENT);

        when(envelopMock.getMessage()).thenReturn(gson.fromJson(event.json, Map.class));

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.process(envelopMock, collectorMock, coordinatorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), SUCCESS_TOPIC)));
        verify(counterMock).inc();
    }

    @Test
    public void shouldSkipOtherEvents() throws Exception {
        Gson gson = new Gson();
        Event event = new Event(new EventFixture().OTHER_EVENT);

        when(envelopMock.getMessage()).thenReturn(gson.fromJson(event.json, Map.class));

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.process(envelopMock, collectorMock, coordinatorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), SUCCESS_TOPIC)));
        verify(counterMock).inc();
    }

    @Test
    public void shouldSkipOtherChannelEvents() throws Exception {
        Gson gson = new Gson();
        Event event = new Event(new EventFixture().OTHER_CHANNEL_EVENT);

        when(envelopMock.getMessage()).thenReturn(gson.fromJson(event.json, Map.class));

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.process(envelopMock, collectorMock, coordinatorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), SUCCESS_TOPIC)));
        verify(counterMock).inc();
    }

    @Test
    public void shouldSendEventsToFailedTopicIfUidIsEmpty() throws Exception {
        Gson gson = new Gson();
        Event event = new Event(new EventFixture().EVENT_WITHOUT_UID);

        when(envelopMock.getMessage()).thenReturn(gson.fromJson(event.json, Map.class));

        UserManagementTask userManagementTask = new UserManagementTask();
        userManagementTask.init(configMock, contextMock);
        userManagementTask.process(envelopMock, collectorMock, coordinatorMock);

        verify(collectorMock).send(argThat(validateOutputTopic(event.getMap(), FAILURE_TOPIC)));
    }

    private ArgumentMatcher<OutgoingMessageEnvelope> validateOutputTopic(final Object message, final String stream) {
        return new ArgumentMatcher<OutgoingMessageEnvelope>() {
            @Override
            public boolean matches(Object o) {
                OutgoingMessageEnvelope outgoingMessageEnvelope = (OutgoingMessageEnvelope) o;
                SystemStream systemStream = outgoingMessageEnvelope.getSystemStream();
                assertEquals("kafka", systemStream.getSystem());
                assertEquals(stream, systemStream.getStream());
                assertEquals(message, outgoingMessageEnvelope.getMessage());
                return true;
            }
        };
    }
}

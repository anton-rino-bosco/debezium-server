/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.PubsubMessage;

import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for {@link PubSubChangeConsumer} header → attribute mapping.
 */
class PubSubChangeConsumerTest {

    private PubSubChangeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PubSubChangeConsumer();
        consumer.config = new PubSubChangeConsumerConfig(Configuration.create()
                .with(PubSubChangeConsumerConfig.ORDERING_ENABLED, false)
                .build());
        consumer.headerSchemasEnabled = true;
    }

    @Test
    void testHeadersAsAttributesWithSchemasEnabledUsesConverterOutput() {
        consumer.headerSchemasEnabled = true;

        String envelope = "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"OrderCreated\"}";
        BatchEvent event = createEventWithHeaders(
                Map.of("eventType", "OrderCreated"),
                Map.of("eventType", envelope));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("eventType", envelope);
    }

    @Test
    void testHeadersAsAttributesWithSchemasDisabledUsesRawConnectValues() {
        consumer.headerSchemasEnabled = false;

        // JsonConverter with schemas.enable=false encodes string headers as JSON strings
        // (with surrounding quotes). Raw Connect headers must be used instead.
        BatchEvent event = createEventWithHeaders(
                Map.of("op", "c"),
                Map.of("op", "\"c\""));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsExactly(Map.entry("op", "c"));
        assertThat(message.getAttributesMap().get("op")).doesNotContain("\"");
    }

    @Test
    void testHeadersAsAttributesWithSchemasDisabledForwardsMultipleHeaders() {
        consumer.headerSchemasEnabled = false;

        BatchEvent event = createEventWithHeaders(
                Map.of("eventType", "OrderCreated", "requestType", "command"),
                Map.of("eventType", "\"OrderCreated\"", "requestType", "\"command\""));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap())
                .containsEntry("eventType", "OrderCreated")
                .containsEntry("requestType", "command");
    }

    @Test
    void testHeadersAsAttributesWithSchemasDisabledHandlesByteArrayHeader() {
        consumer.headerSchemasEnabled = false;

        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("rawByteHeader", "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.headers()).thenReturn(headers);

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn("{\"id\":1}");
        when(event.destination()).thenReturn("testc.inventory.customers");
        when(event.record()).thenReturn(sourceRecord);
        when(event.headers()).thenReturn(List.of());

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("rawByteHeader", "hello");
    }

    @Test
    void testHeadersAsAttributesWithSchemasDisabledHandlesNullRecordGracefully() {
        consumer.headerSchemasEnabled = false;

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn("{\"id\":1}");
        when(event.destination()).thenReturn("testc.inventory.customers");
        when(event.record()).thenReturn(null);
        when(event.headers()).thenReturn(List.of());

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).isEmpty();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createEventWithHeaders(Map<String, String> connectHeaders,
                                                     Map<String, String> changeEventHeaders) {
        ConnectHeaders headers = new ConnectHeaders();
        connectHeaders.forEach(headers::addString);

        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.headers()).thenReturn(headers);
        when(sourceRecord.key()).thenReturn("key");
        when(sourceRecord.value()).thenReturn("{\"id\":1}");

        List<Header<Object>> serializedHeaders = new ArrayList<>();
        changeEventHeaders.forEach((key, value) -> {
            Header<Object> header = mock(Header.class);
            when(header.getKey()).thenReturn(key);
            when(header.getValue()).thenReturn(value);
            serializedHeaders.add(header);
        });

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn("{\"id\":1}");
        when(event.destination()).thenReturn("testc.inventory.customers");
        when(event.record()).thenReturn(sourceRecord);
        when(event.headers()).thenReturn(serializedHeaders);
        return event;
    }
}

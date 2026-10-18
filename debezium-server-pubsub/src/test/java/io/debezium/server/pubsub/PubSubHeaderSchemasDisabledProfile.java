/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Runs the Pub/Sub sink integration test with {@code debezium.format.header.schemas.enable=false}. In that
 * configuration the header converter serializes a string header as a JSON string, so the sink has to fall
 * back to the raw Connect header value.
 *
 * <p>See <a href="https://github.com/debezium/dbz/issues/2783">DBZ issue #2783</a>.
 */
public class PubSubHeaderSchemasDisabledProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("debezium.format.header.schemas.enable", "false");
    }
}

/**
 * Copyright 2017 Pivotal Software, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.opentsdb;

import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.lang.Nullable;

/**
 * Configuration for {@link OpenTsdbMeterRegistry}.
 *
 * @author Jon Schneider
 */
public interface OpenTsdbConfig extends StepRegistryConfig {
    /**
     * Accept configuration defaults
     */
    OpenTsdbConfig DEFAULT = k -> null;

    @Override
    default String prefix() {
        return "opentsdb";
    }

    /**
     * @return Authenticate requests with this user. By default is {@code null}, and the registry will not
     * attempt to present credentials to Influx.
     */
    @Nullable
    default String userName() {
        return get(prefix() + ".userName");
    }

    /**
     * @return Authenticate requests with this password. By default is {@code null}, and the registry will not
     * attempt to present credentials to Influx.
     */
    @Nullable
    default String password() {
        return get(prefix() + ".password");
    }

    /**
     * @return The URI for the Influx backend. The default is {@code http://localhost:8086}.
     */
    default String uri() {
        String v = get(prefix() + ".uri");
        return (v == null) ? "http://localhost:8086" : v;
    }

    /**
     * @return {@code true} if metrics publish batches should be GZIP compressed, {@code false} otherwise.
     */
    default boolean compressed() {
        String v = get(prefix() + ".compressed");
        return v == null || Boolean.valueOf(v);
    }

}

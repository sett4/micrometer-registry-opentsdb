/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.opentsdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.StringEscapeUtils;

/**
 * Representation of a metric.
 *
 * @author Sean Scanlon &lt;sean.scanlon@gmail.com&gt;
 * @author Adam Lugowski &lt;adam.lugowski@turn.com&gt;
 *
 */
public class OpenTsdbMetric {
    private String metric;

    private Long timestamp;

    private Object value;

    private List<Tag> tags = new ArrayList<>();

    private OpenTsdbMetric() {
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof OpenTsdbMetric)) {
            return false;
        }

        final OpenTsdbMetric rhs = (OpenTsdbMetric) o;

        return equals(metric, rhs.metric)
               && equals(timestamp, rhs.timestamp)
               && equals(value, rhs.value)
               && equals(tags, rhs.tags);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] { metric, timestamp, value, tags });
    }

    /**
     * Returns a JSON string version of this metric compatible with the HTTP API reporter.
     *
     * Example:
     * <pre><code>
     * {
     *     "metric": "sys.cpu.nice",
     *     "timestamp": 1346846400,
     *     "value": 18,
     *     "tags": {
     *         "host": "web01",
     *         "dc": "lga"
     *     }
     * }
     * </code></pre>
     * @return a JSON string version of this metric compatible with the HTTP API reporter.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName()
               + "->metric: " + metric
               + ",value: " + value
               + ",timestamp: " + timestamp
               + ",tags: " + tags;
    }

    public String toJsonString(NamingConvention namingConvention) {
        StringBuilder openTsdbJson = new StringBuilder();

        openTsdbJson.append("{");

        openTsdbJson.append("\"metric\":\"")
                .append(StringEscapeUtils.escapeJson(namingConvention.name(getMetric(), Meter.Type.OTHER)))
                .append("\",");
        openTsdbJson.append("\"timestamp\":").append(getTimestamp()).append(",");

        String s = getValue().toString();
        if ("Infinite".equals(s)) {
            s = "1E400";
        } else if ("-Infinite".equals(s)) {
            s = "-1E400";
        }

        openTsdbJson.append("\"value\":")
                .append(StringEscapeUtils.escapeJson(s))
                .append(",");

        openTsdbJson.append("\"tags\":{");

        if (getTags() != null) {
            Iterator<Tag> entryIterator = getTags().iterator();
            while (entryIterator.hasNext()) {
                Tag tag = entryIterator.next();

                openTsdbJson.append("\"").append(StringEscapeUtils.escapeJson(namingConvention.tagKey(tag.getKey())));
                openTsdbJson.append("\":\"").append(StringEscapeUtils.escapeJson(namingConvention.tagValue(tag.getValue())))
                            .append("\"");

                if (entryIterator.hasNext()) { openTsdbJson.append(","); }
            }
        }

        openTsdbJson.append("}");

        openTsdbJson.append("}");

        return openTsdbJson.toString();
    }

    public String getMetric() {
        return metric;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public List<Tag> getTags() {
        return tags;
    }

    private boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

    public static class Builder {

        private final OpenTsdbMetric metric;

        public Builder(String name) {
            this.metric = new OpenTsdbMetric();
            metric.metric = name;
        }

        public OpenTsdbMetric build() {
            return metric;
        }

        public Builder withValue(Object value) {
            metric.value = value;
            return this;
        }

        public Builder withTimestamp(Long timestamp) {
            metric.timestamp = timestamp;
            return this;
        }

        public Builder withTags(List<Tag> tags) {
            if (tags != null) {
                metric.tags.addAll(tags);
            }
            return this;
        }
    }
}

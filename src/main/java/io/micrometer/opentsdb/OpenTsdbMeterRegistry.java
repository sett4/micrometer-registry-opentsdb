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

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.*;
import io.micrometer.core.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * @author Jon Schneider
 */
public class OpenTsdbMeterRegistry extends StepMeterRegistry {
    private final OpenTsdbConfig config;
    private final Logger logger = LoggerFactory.getLogger(OpenTsdbMeterRegistry.class);

    @FunctionalInterface
    public interface AuthenticateRequester {
        void authenticateRequest(HttpURLConnection conn, OpenTsdbConfig config);
    }
    private AuthenticateRequester authenticateRequester;
    public void setAuthenticateRequester(AuthenticateRequester processor) {
        authenticateRequester = processor;
    }

    public OpenTsdbMeterRegistry(OpenTsdbConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        this.config().namingConvention(new OpenTsdbNamingConvention());
        this.config = config;

        start(threadFactory);
    }

    public OpenTsdbMeterRegistry(OpenTsdbConfig config, Clock clock) {
        this(config, clock, Executors.defaultThreadFactory());
    }


    @Override
    protected void publish() {
        try {
            String write = "/api/put";
            URL openTsdbEndpoint = URI.create(config.uri() + write).toURL();
            HttpURLConnection con = null;

            for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
                try {
                    con = (HttpURLConnection) openTsdbEndpoint.openConnection();
                    con.setConnectTimeout((int) config.connectTimeout().toMillis());
                    con.setReadTimeout((int) config.readTimeout().toMillis());
                    con.setRequestMethod("POST");
                    con.setRequestProperty("Content-Type", "application/json");
                    con.setDoOutput(true);

                    authenticateRequest(con);

                    List<String> bodyLines = batch.stream()
                            .flatMap(m -> {
                                if (m instanceof Timer) {
                                    return writeTimer((Timer) m);
                                }
                                if (m instanceof DistributionSummary) {
                                    return writeSummary((DistributionSummary) m);
                                }
                                if (m instanceof FunctionTimer) {
                                    return writeTimer((FunctionTimer) m);
                                }
                                if (m instanceof TimeGauge) {
                                    return writeGauge(m.getId(), ((TimeGauge) m).value(getBaseTimeUnit()));
                                }
                                if (m instanceof Gauge) {
                                    return writeGauge(m.getId(), ((Gauge) m).value());
                                }
                                if (m instanceof FunctionCounter) {
                                    return writeCounter(m.getId(), ((FunctionCounter) m).count());
                                }
                                if (m instanceof Counter) {
                                    return writeCounter(m.getId(), ((Counter) m).count());
                                }
                                if (m instanceof LongTaskTimer) {
                                    return writeLongTaskTimer((LongTaskTimer) m);
                                }
                                return writeMeter(m);
                            })
                            .collect(toList());

                    StringBuilder body = new StringBuilder();
                    body.append("[");
                    body.append(String.join(",", bodyLines));
                    body.append("]");

                    if (config.compressed())
                        con.setRequestProperty("Content-Encoding", "gzip");

                    try (OutputStream os = con.getOutputStream()) {
                        if (config.compressed()) {
                            try (GZIPOutputStream gz = new GZIPOutputStream(os)) {
                                gz.write(body.toString().getBytes());
                                gz.flush();
                            }
                        } else {
                            os.write(body.toString().getBytes());
                        }
                        os.flush();
                    }

                    int status = con.getResponseCode();

                    if (status >= 200 && status < 300) {
                        logger.debug("successfully sent {} metrics to OpenTSDB", batch.size());
                    } else if (status >= 400) {
                        if (logger.isErrorEnabled()) {
                            try (InputStream in = con.getErrorStream()) {
                                logger.error("failed to send metrics: " + new BufferedReader(new InputStreamReader(in))
                                    .lines().collect(joining("\n")));
                            }                        }
                    } else {
                        logger.error("failed to send metrics: http {}", status);
                    }

                } finally {
                    quietlyCloseUrlConnection(con);
                }
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed OpenTSDB publishing endpoint, see '" + config.prefix() + ".uri'", e);
        } catch (Throwable e) {
            logger.error("failed to send metrics", e);
        }
    }

    private void authenticateRequest(HttpURLConnection con) {
        if (config.userName() != null && config.password() != null && authenticateRequester != null) {
            authenticateRequester.authenticateRequest(con, config);
        }
    }

    private void quietlyCloseUrlConnection(@Nullable HttpURLConnection con) {
        try {
            if (con != null) {
                con.disconnect();
            }
        } catch (Exception ignore) {
        }
    }

    class Field {
        final String key;
        final double value;

        Field(String key, double value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "=" + DoubleFormat.decimalOrNan(value);
        }
    }

    private Stream<String> writeMeter(Meter m) {
        long wallTime = clock.wallTime();
        return StreamSupport.stream(m.measure().spliterator(), false)
                            .map(measurement -> {
            String suffix = measurement.getStatistic().toString()
                                         .replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
            return openTsdbJson(m.getId(), suffix, "unknown", measurement.getValue(), wallTime);
        });
    }

    private Stream<String> writeLongTaskTimer(LongTaskTimer timer) {
        long wallTime = clock.wallTime();
        return Stream.of(
            openTsdbJson(timer.getId(), "active_tasks", "long_task_timer", timer.activeTasks(), wallTime),
            openTsdbJson(timer.getId(), "duration", "long_task_timer", timer.duration(getBaseTimeUnit()), wallTime)
        );
    }

    private Stream<String> writeCounter(Meter.Id id, Double count) {
        return Stream.of(openTsdbJson(id, null, "counter", count, clock.wallTime()));
    }

    private Stream<String> writeGauge(Meter.Id id, Double value) {
        return value.isNaN() ? Stream.empty() :
                Stream.of(openTsdbJson(id, null, "gauge", value, clock.wallTime()));
    }

    private Stream<String> writeTimer(FunctionTimer timer) {
        long wallTime = clock.wallTime();
        return Stream.of(
                openTsdbJson(timer.getId(), "sum", "histogram", timer.totalTime(getBaseTimeUnit()), wallTime),
                openTsdbJson(timer.getId(), "count", "histogram", timer.count(), wallTime),
                openTsdbJson(timer.getId(), "mean", "histogram", timer.mean(getBaseTimeUnit()), wallTime)
        );
    }

    private Stream<String> writeTimer(Timer timer) {
        long wallTime = clock.wallTime();
        return Stream.of(
                openTsdbJson(timer.getId(), "sum", "histogram", timer.totalTime(getBaseTimeUnit()), wallTime),
                openTsdbJson(timer.getId(), "count", "histogram", timer.count(), wallTime),
                openTsdbJson(timer.getId(), "mean", "histogram", timer.mean(getBaseTimeUnit()), wallTime),
                openTsdbJson(timer.getId(), "upper", "histogram", timer.max(getBaseTimeUnit()), wallTime)
        );
    }

    private Stream<String> writeSummary(DistributionSummary summary) {
        long wallTime = clock.wallTime();
        return Stream.of(
                openTsdbJson(summary.getId(), "sum", "histogram", summary.totalAmount(), wallTime),
                openTsdbJson(summary.getId(), "count", "histogram", summary.count(), wallTime),
                openTsdbJson(summary.getId(), "mean", "histogram", summary.mean(), wallTime),
                openTsdbJson(summary.getId(), "upper", "histogram", summary.max(), wallTime)
        );
    }

    private String openTsdbJson(Meter.Id id, @Nullable String suffix, String metricType, Object value, long time) {
        Meter.Id fullId = id;
        if (suffix != null) {
            fullId = idWithSuffix(id, suffix);
        }
        List<Tag> tags = getConventionTags(fullId);
        return new OpenTsdbMetric.Builder(getConventionName(fullId))
                .withTags(tags)
                .withTimestamp(time)
                .withValue(value)
                .build()
                .toJsonString(config().namingConvention());
    }

    @Override
    protected final TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    /**
     * Copy tags, unit, and description from an existing id, but change the name.
     */
    private Meter.Id idWithSuffix(Meter.Id id, String suffix) {
        return new Meter.Id(id.getName() + "." + suffix, id.getTags(), id.getBaseUnit(), id.getDescription(), id.getType());
    }

}

package io.neonbee.data.internal;

import static com.google.common.truth.Truth.assertThat;
import static io.neonbee.data.ResponseMetadataPropagationPolicy.AUTO_MERGE;
import static io.neonbee.data.ResponseMetadataPropagationPolicy.MANUAL_PROCESS;
import static io.vertx.core.Future.succeededFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.neonbee.NeonBeeDeployable;
import io.neonbee.data.DataContext;
import io.neonbee.data.DataMap;
import io.neonbee.data.DataQuery;
import io.neonbee.data.DataRequest;
import io.neonbee.data.DataVerticle;
import io.neonbee.data.ResponseMetadataPropagationPolicy;
import io.neonbee.test.base.DataVerticleTestBase;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

class ResponseMetadataIntegrationTest extends DataVerticleTestBase {
    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Check that response metadata is properly propagated")
    void testResponseMetadataPropagation(VertxTestContext testContext) {
        DataContext dataContext =
                new DataContextImpl("corr", "sess", "bearer", new JsonObject(), Map.of("key", "value"));
        DataRequest request = new DataRequest("Caller", new DataQuery());
        deployVerticle(new DataVerticleCallee(AUTO_MERGE))
                .compose(de -> deployVerticle(new DataVerticleCaller(AUTO_MERGE)))
                .compose(de -> deployVerticle(new DataVerticleIntermediary(AUTO_MERGE)))
                .compose(de -> requestData(request, dataContext))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertThat(result).isEqualTo("Response from caller");
                    assertThat(dataContext.<String>getResponseMetadataEntry("calleeHint")).isEqualTo("Callee");
                    assertThat(dataContext.<String>getResponseMetadataEntry("intermediaryHint"))
                            .isEqualTo("Intermediary");
                    assertThat(dataContext.<String>getResponseMetadataEntry("callerHint")).isEqualTo("Caller");
                    assertThat(dataContext.<String>getResponseMetadataEntry("contentType")).isEqualTo("YML");
                    testContext.completeNow();
                })));
    }

    @Test
    @Timeout(value = 200, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Check that response metadata should not be propagated")
    void testResponseMetadataNoPropagation(VertxTestContext testContext) {
        DataContext dataContext =
                new DataContextImpl("corr", "sess", "bearer", new JsonObject(), Map.of("key", "value"));
        DataRequest request = new DataRequest("Caller", new DataQuery());
        deployVerticle(new DataVerticleCallee(MANUAL_PROCESS))
                .compose(de -> deployVerticle(new DataVerticleIntermediary(MANUAL_PROCESS)))
                .compose(de -> deployVerticle(new DataVerticleCaller(AUTO_MERGE)))
                .compose(de -> requestData(request, dataContext))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertThat(result).isEqualTo("Response from caller");
                    assertThat(dataContext.<String>getResponseMetadataEntry("calleeHint")).isNull();
                    assertThat(dataContext.<String>getResponseMetadataEntry("intermediaryHint"))
                            .isEqualTo("Intermediary");
                    assertThat(dataContext.<String>getResponseMetadataEntry("callerHint")).isEqualTo("Caller");
                    assertThat(dataContext.<String>getResponseMetadataEntry("contentType")).isEqualTo("YML");
                    testContext.completeNow();
                })));
    }

    @NeonBeeDeployable
    private static class DataVerticleCallee extends DataVerticle<String> {
        public static final String NAME = "Callee";

        private final ResponseMetadataPropagationPolicy policy;

        DataVerticleCallee(ResponseMetadataPropagationPolicy policy) {
            super();
            this.policy = policy;
        }

        @Override
        public ResponseMetadataPropagationPolicy getResponseMetadataPropagationPolicy() {
            return this.policy;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Future<String> retrieveData(DataQuery query, DataMap require, DataContext context) {
            context.putResponseMetadataEntry("calleeHint", "Callee");
            context.putResponseMetadataEntry("contentType", "JSON");
            return succeededFuture("Response from callee");
        }
    }

    @NeonBeeDeployable
    private static class DataVerticleIntermediary extends DataVerticle<String> {
        public static final String NAME = "Intermediary";

        private final ResponseMetadataPropagationPolicy policy;

        DataVerticleIntermediary(ResponseMetadataPropagationPolicy policy) {
            super();
            this.policy = policy;
        }

        @Override
        public ResponseMetadataPropagationPolicy getResponseMetadataPropagationPolicy() {
            return this.policy;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Future<Collection<DataRequest>> requireData(DataQuery query, DataContext context) {
            return succeededFuture(List.of(new DataRequest("Callee")));
        }

        @Override
        public Future<String> retrieveData(DataQuery query, DataMap require, DataContext context) {
            context.putResponseMetadataEntry("intermediaryHint", "Intermediary");
            context.putResponseMetadataEntry("contentType", "XML");
            return succeededFuture("Response from intermediary");
        }
    }

    @NeonBeeDeployable
    private static class DataVerticleCaller extends DataVerticle<String> {
        public static final String NAME = "Caller";

        private final ResponseMetadataPropagationPolicy policy;

        DataVerticleCaller(ResponseMetadataPropagationPolicy policy) {
            super();
            this.policy = policy;
        }

        @Override
        public ResponseMetadataPropagationPolicy getResponseMetadataPropagationPolicy() {
            return this.policy;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Future<Collection<DataRequest>> requireData(DataQuery query, DataContext context) {
            return succeededFuture(List.of(new DataRequest("Intermediary")));
        }

        @Override
        public Future<String> retrieveData(DataQuery query, DataMap require, DataContext context) {
            context.putResponseMetadataEntry("callerHint", "Caller");
            context.putResponseMetadataEntry("contentType", "YML");
            return succeededFuture("Response from caller");
        }
    }

}

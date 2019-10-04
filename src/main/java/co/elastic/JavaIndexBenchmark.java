package co.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

//curl -X DELETE localhost:9200/myindex
//mvn clean package; java -jar target/benchmarks.jar
//curl -v localhost:9200/myindex/_count

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 1, time = 30)
@Fork(value = 3)
public class JavaIndexBenchmark {

    private static final int ITEMS_PER_BULK = 20000;
    private static final int CONCURRENT_BULKS_IN_FLIGHT = 6;

    @State(Scope.Benchmark)
    public static class JMHIndexRequest {
        private final IndexRequest request;

        public JMHIndexRequest() {
            request = new IndexRequest("myindex");
            request.source(Map.of("foo", "bar"));
        }
    }

    @State(Scope.Benchmark)
    public static class JMHTransportBulkProcessor {

        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger docCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final Settings settings;
        private final TransportClient client;
        private  BulkRequestBuilder bulkRequestBuilder;
        private final Semaphore inFlightThrottle = new Semaphore(CONCURRENT_BULKS_IN_FLIGHT);

        public JMHTransportBulkProcessor() {
            settings = Settings.builder().put("cluster.name", "docker-cluster").build();
            InetAddress host;
            try {
                host = InetAddress.getByName("localhost");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(host, 9300))
                    .addTransportAddress(new TransportAddress(host, 9301));
            bulkRequestBuilder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
        }

        //simulate the Rest Bulk Processor
        public void add(IndexRequest request) throws InterruptedException {
            bulkRequestBuilder.add(request);
            docCount.incrementAndGet();
            if (bulkRequestBuilder.numberOfActions() == ITEMS_PER_BULK) {
                BulkRequestBuilder localBuilder =  this.bulkRequestBuilder;
                this.bulkRequestBuilder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
                inFlightThrottle.acquire();
                ActionFuture<BulkResponse> responseActionFuture = client.bulk(localBuilder.request());
                //wait for the response on a thread and once returned release a slot in the throttle
                new Thread( () -> {
                    BulkResponse responses = responseActionFuture.actionGet();
                    if(responses.hasFailures()){
                        failureCount.incrementAndGet();
                    }else{
                        successCount.incrementAndGet();
                    }
                    inFlightThrottle.release();
                }).start();

            }
        }

        @TearDown
        public void tearDown() {
            System.out.println("***********************");
            System.out.println("Documents sent: " + docCount);
            System.out.println("Successful Bulks: " + successCount);
            System.out.println("Failed Bulks: " + failureCount);
            System.out.println("***********************");
          //  client.close();  //causes errors to show in output that are irrelevant to the test
        }
    }

    @State(Scope.Benchmark)
    public static class JMHRestBulkProcessor {
        private final BulkProcessor bulkProcessor;
        private final RestHighLevelClient client;
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger docCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);

        public JMHRestBulkProcessor() {
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost("localhost", 9200, "http"),
                    new HttpHost("localhost", 9201, "http")));

            BulkProcessor.Builder builder = BulkProcessor.builder(
                    (request, bulkListener) ->
                            client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long executionId, BulkRequest request) {
                            docCount.addAndGet(request.numberOfActions());
                        }

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                            successCount.incrementAndGet();
                        }

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                            failureCount.incrementAndGet();
                        }
                    });
            bulkProcessor = builder.setBulkActions(ITEMS_PER_BULK)
                    .setBulkSize(new ByteSizeValue(500, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueMinutes(5))
                    .setConcurrentRequests(CONCURRENT_BULKS_IN_FLIGHT)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
        }

        @TearDown
        public void tearDown() throws Exception {
            System.out.println("***********************");
            System.out.println("Documents sent: " + docCount);
            System.out.println("Successful Bulks: " + successCount);
            System.out.println("Failed Bulks: " + failureCount);
            System.out.println("***********************");
            client.close();
        }
    }


    @Benchmark
    public void indexWithRestClient(JMHRestBulkProcessor jmhRestBulkProcessor, JMHIndexRequest jmhIndexRequest) {
        jmhRestBulkProcessor.bulkProcessor.add(jmhIndexRequest.request);
    }

    @Benchmark
    public void indexWithTransportClient(JMHTransportBulkProcessor jmhTransportBulkProcessor, JMHIndexRequest jmhIndexRequest) throws InterruptedException {
        jmhTransportBulkProcessor.add(jmhIndexRequest.request);
    }

}

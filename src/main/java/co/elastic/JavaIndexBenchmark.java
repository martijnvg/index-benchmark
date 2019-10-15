package co.elastic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.DeleteIndex;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private static final String INDEX_NAME = "myindex";
    private static final int ITEMS_PER_BULK = 20000;
    private static final int CONCURRENT_BULKS_IN_FLIGHT = 6;

    @State(Scope.Benchmark)
    public static class JMHIndexRequest {
        private final IndexRequest request;

        public JMHIndexRequest() {
            request = new IndexRequest(INDEX_NAME);
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
                    .addTransportAddress(new TransportAddress(host, 9300)
                    //).addTransportAddress(new TransportAddress(host, 9301)
                    );
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
                    new HttpHost("localhost", 9200, "http")
            //       , new HttpHost("localhost", 9201, "http")
            ));

            BulkProcessor.Builder builder = BulkProcessor.builder(
                    (request, bulkListener) ->
                            client.bulkAsync(request/*, RequestOptions.DEFAULT*/, bulkListener), new BulkProcessor.Listener() {
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

    @State(Scope.Benchmark)
    public static class Doc {
        private final Map<String, Object> doc;
        private final String docAsString;

        public Doc() {
            this.doc = /*Map.of("foo", "bar");*/createDoc(20, 2);
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.map(doc);
                this.docAsString = Strings.toString(builder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static Map<String, Object> createDoc(int numFields, int levels) {
            Map<String, Object> source = new HashMap<>(numFields);
            for (int i = 0; i < numFields; i++) {
                if (levels == 0) {
                    source.put("field" + i, i % 2 == 0 ? "value" + i : i);
                } else {
                    source.put("field" + i, createDoc(5, levels - 1));
                }
            }
            return source;
        }
    }

    @State(Scope.Benchmark)
    public static class HLRCBulkIndexing {

        private final RestHighLevelClient client;
        private BulkRequest request;

        private final Semaphore semaphore = new Semaphore(1);
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger docCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);

        public HLRCBulkIndexing() {
            final CredentialsProvider credentialsProvider = createCredentialsProvider();

            final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setConnectTimeout(0)
                    .build();

            final Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                    .register("http", NoopIOSessionStrategy.INSTANCE)
                    .register("https", SSLIOSessionStrategy.getSystemDefaultStrategy())
                    .build();

            final RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback = new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                    try {
                        return httpClientBuilder.setConnectionManager(new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(ioReactorConfig), sessionStrategyRegistry))
                                /*.setDefaultCredentialsProvider(credentialsProvider)*/;
                    } catch (IOReactorException e) {
                        throw new RuntimeException("Exception while setting the PoolingNHttpClient Connection Manager", e);
                    }
                }
            };

            final RestClientBuilder.RequestConfigCallback requestConfigCallback = new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(
                        RequestConfig.Builder requestConfigBuilder) {
                    return requestConfigBuilder.setConnectTimeout(0);
                }
            };

            client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        }

        public void index(String data) throws InterruptedException {
            docCount.incrementAndGet();
            IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "_doc");
            indexRequest.source(data, XContentType.JSON);
            if (request == null) {
                request = new BulkRequest();
            }
            request.add(indexRequest);
            if (request.requests().size() >= 5000) {
                flush();
                request = null;
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            System.out.println("***********************");
            System.out.println("Documents sent: " + docCount);
            System.out.println("Successful Bulks: " + successCount);
            System.out.println("Failed Bulks: " + failureCount);
            System.out.println("***********************");
            flush();
            semaphore.acquire();
            try {
                client.indices().delete(new DeleteIndexRequest(INDEX_NAME)/*, RequestOptions.DEFAULT*/);
            } catch (Exception e) {
                // ignore
            }
            client.close();
            semaphore.release();
        }

        private void flush() throws InterruptedException {
            if (request == null) {
                return;
            }

            semaphore.acquire();
            client.bulkAsync(request, /*RequestOptions.DEFAULT,*/ ActionListener.wrap(bulkItemResponses -> {
                semaphore.release();
                if (bulkItemResponses.hasFailures()) {
                    System.out.println(bulkItemResponses.buildFailureMessage());
                    failureCount.incrementAndGet();
                } else {
                    successCount.incrementAndGet();
                }
            }, e -> {
                semaphore.release();
                failureCount.incrementAndGet();
            }));
        }

    }

    @State(Scope.Benchmark)
    public static class JestBulkIndexing {

        private final JestClient client;
        private List<Index> bulk;

        private final Semaphore semaphore = new Semaphore(1);
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger docCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);

        public JestBulkIndexing() {
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig
                    .Builder("http://localhost:9200")
                    .multiThreaded(true)
                    //Per default this implementation will create no more than 2 concurrent connections per given route
                    .defaultMaxTotalConnectionPerRoute(2)
                    // and no more 20 connections in total
                    .connTimeout(0)
                    .readTimeout(0)
                    .maxTotalConnection(2).build());
            client = factory.getObject();
        }

        public void index(String data) throws InterruptedException {
            docCount.incrementAndGet();
            if (bulk == null) {
                bulk = new ArrayList<>(5000);
            }
            Index.Builder index = new Index.Builder(data).index(INDEX_NAME).type("_doc");
            bulk.add(index.build());
            if (bulk.size() >= 5000) {
                flush();
                bulk = null;
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            System.out.println("***********************");
            System.out.println("Documents sent: " + docCount);
            System.out.println("Successful Bulks: " + successCount);
            System.out.println("Failed Bulks: " + failureCount);
            System.out.println("***********************");
            flush();
            semaphore.acquire();
            client.execute(new DeleteIndex.Builder(INDEX_NAME).build());
            client.close();
            semaphore.release();
        }

        private void flush() throws InterruptedException {
            if (bulk == null) {
                return;
            }

            Bulk.Builder bulkBuilder = new Bulk.Builder();
            bulkBuilder.addAction(bulk);
            semaphore.acquire();
            client.executeAsync(bulkBuilder.build(), new JestResultHandler<>() {
                @Override
                public void completed(BulkResult bulkResult) {
                    semaphore.release();
                    successCount.incrementAndGet();
                }

                @Override
                public void failed(Exception e) {
                    e.printStackTrace();
                    semaphore.release();
                    failureCount.incrementAndGet();
                }
            });
        }

    }

//    @Benchmark
    public void indexWithRestClient(JMHRestBulkProcessor jmhRestBulkProcessor, JMHIndexRequest jmhIndexRequest) {
        jmhRestBulkProcessor.bulkProcessor.add(jmhIndexRequest.request);
    }

//    @Benchmark
    public void indexWithTransportClient(JMHTransportBulkProcessor jmhTransportBulkProcessor, JMHIndexRequest jmhIndexRequest) throws InterruptedException {
        jmhTransportBulkProcessor.add(jmhIndexRequest.request);
    }

    @Benchmark
    public void indexWithHLRC(HLRCBulkIndexing hlrcBulkIndexing, Doc doc) throws InterruptedException {
        hlrcBulkIndexing.index(doc.docAsString);
    }

    @Benchmark
    public void indexWithJest(JestBulkIndexing jestBulkIndexing, Doc doc) throws InterruptedException {
        jestBulkIndexing.index(doc.docAsString);
    }

}

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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Runner {

    private final static Doc DOC = new Doc();

    public static class HLRC {

        public static void main(String[] args) throws Exception {
            final Semaphore semaphore = new Semaphore(1);
            final RestHighLevelClient client =
                    new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

            try {
                client.indices().delete(new DeleteIndexRequest("my-index")/*, RequestOptions.DEFAULT*/);
            } catch (Exception e) {
                // ignore
            }

            long startTime = System.currentTimeMillis();
            BulkRequest bulkRequest = new BulkRequest();
            for (int id = 0; id < 50000; id++) {
                bulkRequest.add(new IndexRequest("my-index", "_doc").source(DOC.docAsString, XContentType.JSON));
                if (bulkRequest.requests().size() == 1000) {
                    semaphore.acquire();
                    client.bulkAsync(bulkRequest/*, RequestOptions.DEFAULT*/, ActionListener.wrap(r -> {
                        semaphore.release();
                    }, e -> {
                        semaphore.release();
                        e.printStackTrace();
                    }));
                    bulkRequest = new BulkRequest();
                }
            }
            semaphore.acquire();
            long took = System.currentTimeMillis() - startTime;
            System.out.println("TOOK=" + took);
            client.close();
        }

    }

    public static class Jest {

        public static void main(String[] args) throws Exception {
            final Semaphore semaphore = new Semaphore(1);
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
            final JestClient client = factory.getObject();

            try {
                client.execute(new DeleteIndex.Builder("my-index").type("_doc").build());
            } catch (Exception e) {
                // ignore
            }

            long startTime = System.currentTimeMillis();
            List<Index> requests = new ArrayList<>(1000);
            for (int id = 0; id < 50000; id++) {
                Index.Builder index = new Index.Builder(DOC.docAsString).index("my-index").type("_doc");
                requests.add(index.build());
                if (requests.size() == 1000) {
                    semaphore.acquire();
                    Bulk.Builder bulkBuilder = new Bulk.Builder();
                    bulkBuilder.addAction(requests);
                    client.executeAsync(bulkBuilder.build(), new JestResultHandler<>() {
                        @Override
                        public void completed(BulkResult bulkResult) {
                            semaphore.release();
                        }

                        @Override
                        public void failed(Exception e) {
                            e.printStackTrace();
                            semaphore.release();
                        }
                    });
                    requests = new ArrayList<>(1000);
                }
            }
            semaphore.acquire();
            long took = System.currentTimeMillis() - startTime;
            System.out.println("TOOK=" + took);
            client.close();
        }

    }

    public static class Doc {
        private final Map<String, Object> doc;
        private final String docAsString;

        public Doc() {
            this.doc = createDoc(50, 1);
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

}

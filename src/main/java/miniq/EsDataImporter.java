package miniq;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class EsDataImporter {

    private static TransportClient client;
    // a test record item
    private static String item;

    static {
        try {
            item = getItemJson();
            client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        upload100kTestData("schemaless");
        upload100kTestData("schemaful");
        uploadWithRouting("routing");
    }

    private static void upload100kTestData(String indexName) throws URISyntaxException, IOException {
        long startTime = System.nanoTime();
        int id = 0;
        for(int i=0; i < 100; i++) {
            BulkRequestBuilder bulkRequest = client.prepareBulk(indexName, "item");
            for(int y=0; y < 1000; y++) {
                bulkRequest.add(
                        client.prepareIndex().setId(String.valueOf(id++))
                                .setSource(item, XContentType.JSON)
                );
            }

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            if (i > 0 && i % 10 == 0) {
                System.out.println(i + "% done...");
            }
        }
        System.out.println("100% done...\n");
        System.out.println("Took " + (System.nanoTime() - startTime) / 1_000_000_000.0 + " seconds.");
        System.out.println("Well done!\n\n");
    }

    private static void uploadWithRouting(String indexName) throws IOException, URISyntaxException {
        int id = 0;
        for(int i=0; i < 100; i++) {
            BulkRequestBuilder bulkRequest = client.prepareBulk(indexName, "item");
            for(int y=0; y < 500; y++) {
                bulkRequest.add(
                        client.prepareIndex().setId(String.valueOf(id++)).setRouting("1")
                                .setSource(item, XContentType.JSON)
                );
            }

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            if (i > 0 && i % 10 == 0) {
                System.out.println(i + "% done...");
            }
        }
        System.out.println("Routing 1: 100% done...\n");

        for(int i=0; i < 100; i++) {
            BulkRequestBuilder bulkRequest = client.prepareBulk(indexName, "item");
            for(int y=0; y < 500; y++) {
                bulkRequest.add(
                        client.prepareIndex().setId(String.valueOf(id++)).setRouting("2")
                                .setSource(item, XContentType.JSON)
                );
            }

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            if (i > 0 && i % 10 == 0) {
                System.out.println(i + "% done...");
            }
        }
        System.out.println("Routing 2: 100% done...\n");

        System.out.println("Well done!");
    }

    private static String getItemJson() throws URISyntaxException, IOException {
        return new String(
                Files.readAllBytes(
                        Paths.get(
                                Thread.currentThread().getContextClassLoader()
                                        .getResource("complex_item.json").toURI()
                        )
                )
        );
    }

}

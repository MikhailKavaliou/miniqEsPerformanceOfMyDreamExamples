package miniq;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class RoutingExample {

    public static void main(String[] args) throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchRequestBuilder request = client.prepareSearch("routing").setTypes("item")
                .setQuery(QueryBuilders.matchAllQuery()).setRouting("1")
                .setSize(0);

        SearchResponse response = request.get();

        System.out.println("Total hits with routing: " + response.getHits().getTotalHits());

        request = client.prepareSearch("routing").setTypes("item")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0);

        response = request.get();

        System.out.println("Total hits without routing: " + response.getHits().getTotalHits());
    }
}

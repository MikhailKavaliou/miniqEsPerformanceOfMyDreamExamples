package miniq;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class Examples {

    private static SearchRequestBuilder getLimitedWithTimeout(TransportClient client) {
        return client.prepareSearch("schemaful").setTypes("item")
                .setTimeout(TimeValue.timeValueMillis(100))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1);
    }


    private static SearchRequestBuilder getFiltered(TransportClient client) {
        return client.prepareSearch("schemaful").setTypes("item")
                .setQuery(QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("field", "value"))
                                // .must(QueryBuilders.termQuery("field", "value"))
                );
    }


    private static SearchRequestBuilder getAggsOnly(TransportClient client) {
        return client.prepareSearch("schemaful").setTypes("item")
                .addAggregation(AggregationBuilders.terms("agg1").field("fieldName1"))
                .addAggregation(AggregationBuilders.terms("agg2").field("fieldName2"))
                .addAggregation(AggregationBuilders.terms("agg3").field("fieldName3"))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0);
    }


    private static SearchRequestBuilder getProjected(TransportClient client) {
        return client.prepareSearch("schemaful").setTypes("item")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSource(new SearchSourceBuilder()
                        .fetchSource(new String[]{"numericField", "stringField"}, new String[]{})
                );
    }


    public static void main(String[] args) throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        MultiSearchRequestBuilder multiSearchRequest = client.prepareMultiSearch()
                .add(getLimitedWithTimeout(client))
                .add(getAggsOnly(client))
                .add(getFiltered(client))
                .add(getProjected(client));

        MultiSearchResponse response = multiSearchRequest.get(/*TimeValue.timeValueMillis(10)*/);

        for (MultiSearchResponse.Item item : response.getResponses()) {
            System.out.println(item.getResponse());

        }
    }
}

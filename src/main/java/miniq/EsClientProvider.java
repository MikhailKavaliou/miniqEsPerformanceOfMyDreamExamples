package miniq;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class EsClientProvider {

    public static void main(String[] args) throws Exception {
        TransportClient client = new EsClientProvider().initEsClient();
        // ...
    }

    // Coordinating nodes client initiating example.
    public TransportClient initEsClient() throws UnknownHostException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));


        NodesInfoResponse nodesInfo = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
        List<NodeInfo> nodes = nodesInfo.getNodes();
        client.close();

        List<TransportAddress> addresses = new ArrayList<>();
        nodes.stream()
                .filter(nodeInfo -> nodeInfo.getNode().getRoles().isEmpty())
                .forEach(nodeInfo -> addresses.add(nodeInfo.getNode().getAddress()));

        TransportClient elasticClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddresses(addresses.toArray(new TransportAddress[0]));

        return elasticClient;
    }
}

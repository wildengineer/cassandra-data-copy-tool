package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by mgroves on 6/12/16.
 */

@Configuration
public class CassandraConfiguration {

    private final SourceCassandraProperties sourceCassandraProperties;

    private final DestinationCassandraProperties destinationCassandraProperties;
    private final CopyProperties copyProperties;

    @Autowired
    public CassandraConfiguration(CopyProperties copyProperties, SourceCassandraProperties sourceCassandraProperties, DestinationCassandraProperties destinationCassandraProperties) {
        this.sourceCassandraProperties = sourceCassandraProperties;
        this.destinationCassandraProperties = destinationCassandraProperties;
        this.copyProperties = copyProperties;
    }


    @Bean
    public Cluster sourceCluster(SourceCassandraProperties sourceCassandraProperties) {
        Cluster cluster = Cluster.builder()
                .withQueryOptions(new QueryOptions().setFetchSize(copyProperties.getQueryPageSize()))
                .addContactPoints(sourceCassandraProperties.getContactPoints().split(","))
                .withCredentials(sourceCassandraProperties.getUsername(), sourceCassandraProperties.getPassword())
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .withPort(sourceCassandraProperties.getPort())
                .build();

        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 64);
        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE, 16);

        return cluster;
    }

    @Bean
    public Cluster destinationCluster(SourceCassandraProperties destinationCassandraProperties) {
        Cluster cluster = Cluster.builder()
                .withQueryOptions(new QueryOptions().setFetchSize(copyProperties.getQueryPageSize()))
                .addContactPoints(destinationCassandraProperties.getContactPoints().split(","))
                .withCredentials(destinationCassandraProperties.getUsername(), destinationCassandraProperties.getPassword())
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .withPort(destinationCassandraProperties.getPort())
                .build();

        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 64);
        cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE, 16);

        return cluster;
    }

    @Bean
    public Session sourceSession(Cluster sourceCluster) {
        return buildSession(sourceCluster, sourceCassandraProperties);
    }

    @Bean
    public Session destinationSession(Cluster destinationCluster) {
        return buildSession(destinationCluster, destinationCassandraProperties);
    }


    private Session buildSession(Cluster cluster, CassandraProperties cassandraProperties) {
        return cluster.connect(cassandraProperties.getKeyspace());
    }

    @Bean
    public TableMetadata sourceTableMetadata(@Value("${copy.tables}") String sourceTable, Cluster sourceCluster, SourceCassandraProperties sourceCassandraProperties) {
        KeyspaceMetadata keyspace = sourceCluster.getMetadata().getKeyspace(sourceCassandraProperties.getKeyspace());
        return keyspace.getTable(sourceTable);
    }

    @Bean
    public KeyspaceMetadata sourceKeyspaceMetadata(Cluster sourceCluster, SourceCassandraProperties sourceCassandraProperties) {
        return sourceCluster.getMetadata().getKeyspace(sourceCassandraProperties.getKeyspace());
    }
}

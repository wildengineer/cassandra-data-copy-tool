package wildengineer.cassandra.data.copy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Session;

/**
 * Created by mgroves on 6/12/16.
 */

@Configuration
public class CassandraConfiguration {

	@Autowired
	private SourceCassandraProperties sourceCassandraProperties;

	@Autowired
	private DestinationCassandraProperties destinationCassandraProperties;

	@Bean
	public Session sourceSession() {
		return buildSession(sourceCassandraProperties);
	}

	@Bean
	public Session destinationSession() {
		return buildSession(destinationCassandraProperties);
	}

	private Session buildSession(CassandraProperties cassandraProperties) {
		Cluster cluster = Cluster.builder()
				.addContactPoints(cassandraProperties.getContactPoints().split(","))
				.withCredentials(cassandraProperties.getUsername(), cassandraProperties.getPassword())
				.withPort(cassandraProperties.getPort())
				.build();
		cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 64);
		cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE, 16);
		return cluster.connect(cassandraProperties.getKeyspace());
	}
}

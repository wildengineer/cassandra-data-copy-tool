package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by wildengineer on 6/12/16.
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
		//Turning off jmx reporting, since we don't need it and we don't want to bring in any unnecessary dependencies
		//See https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility
		Cluster cluster = Cluster.builder()
				.addContactPoints(cassandraProperties.getContactPoints().split(","))
				.withCredentials(cassandraProperties.getUsername(), cassandraProperties.getPassword())
				.withPort(cassandraProperties.getPort())
				.withoutJMXReporting()
				.build();
		cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 64);
		cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE, 16);
		return cluster.connect(cassandraProperties.getKeyspace());
	}
}

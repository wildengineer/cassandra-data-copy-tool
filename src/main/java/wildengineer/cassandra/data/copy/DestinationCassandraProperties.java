package wildengineer.cassandra.data.copy;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Cassandra connection properties bean for destination connection
 *
 * @author mgroves
 */
@Component("destinationCassandraProperties")
@ConfigurationProperties(prefix = "destination.cassandra")
public class DestinationCassandraProperties extends CassandraProperties {
	//TODO: Add write options
}

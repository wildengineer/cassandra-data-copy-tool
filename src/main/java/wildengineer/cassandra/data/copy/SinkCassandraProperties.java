package wildengineer.cassandra.data.copy;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Cassandra connection properties bean for sink connection
 *
 * @author mgroves
 */
@Component
@ConfigurationProperties(prefix = "sink.cassandra")
public class SinkCassandraProperties extends CassandraProperties {
	//TODO: Add write options
}

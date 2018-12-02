package wildengineer.cassandra.data.copy;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Cassandra connection properties bean for source connection
 *
 * @author wildengineer
 */
@Component
@ConfigurationProperties(prefix = "source.cassandra")
public class SourceCassandraProperties extends CassandraProperties {
	//TODO: Add read options
}

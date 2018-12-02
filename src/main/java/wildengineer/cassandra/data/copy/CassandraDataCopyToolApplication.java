package wildengineer.cassandra.data.copy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration(exclude = {CassandraDataAutoConfiguration.class} )
public class CassandraDataCopyToolApplication {

	public static void main(String[] args) {
		SpringApplication.run(CassandraDataCopyToolApplication.class, args);
	}
}

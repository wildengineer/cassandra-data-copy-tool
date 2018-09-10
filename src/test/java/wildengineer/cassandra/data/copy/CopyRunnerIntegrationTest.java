package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.Session;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by mgroves on 6/4/16.
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@TestExecutionListeners({CassandraUnitDependencyInjectionTestExecutionListener.class,
//		DependencyInjectionTestExecutionListener.class})
//@CassandraDataSet(value = "TestSchemaAndData.cql", keyspace = "test_keyspace")
//@EmbeddedCassandra
//@SpringBootTest(CassandraDataCopyToolApplication.class)
public class CopyRunnerIntegrationTest {

	@Autowired
	private CopyRunner copyRunner;

	@Autowired
	private Session sourceSession;

	@Autowired
	private Session destinationSession;

//	private CassandraTemplate destinationCassandraTemplate;
//
//	private CassandraTemplate sourceCassandraTemplate;
//
//	@Before
//	public void setup() {
//		sourceCassandraTemplate = new CassandraTemplate(sourceSession);
//		destinationCassandraTemplate = new CassandraTemplate(destinationSession);
//	}

	@Test
	public void verify() throws Exception {

		//verify source and destination match
//		verifyUsers(sourceCassandraTemplate, destinationCassandraTemplate);
	}
}

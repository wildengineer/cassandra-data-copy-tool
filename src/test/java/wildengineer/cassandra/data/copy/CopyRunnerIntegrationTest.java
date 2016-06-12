package wildengineer.cassandra.data.copy;

import static wildengineer.cassandra.data.copy.TestUtil.verifyUsers;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitDependencyInjectionTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.datastax.driver.core.Session;

/**
 * Created by mgroves on 6/4/16.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({CassandraUnitDependencyInjectionTestExecutionListener.class,
		DependencyInjectionTestExecutionListener.class})
@CassandraDataSet(value = "TestSchemaAndData.cql", keyspace = "test_keyspace")
@EmbeddedCassandra
@SpringApplicationConfiguration(CassandraDataCopyToolApplication.class)
public class CopyRunnerIntegrationTest {

	@Autowired
	private CopyRunner copyRunner;

	@Autowired
	private Session sourceSession;

	@Autowired
	private Session sinkSession;

	private CassandraTemplate sinkCassandraTemplate;

	private CassandraTemplate sourceCassandraTemplate;

	@Before
	public void setup() {
		sourceCassandraTemplate = new CassandraTemplate(sourceSession);
		sinkCassandraTemplate = new CassandraTemplate(sinkSession);
	}

	@Test
	public void verify() throws Exception {

		//verify source and sink match
		verifyUsers(sourceCassandraTemplate, sinkCassandraTemplate);
	}
}

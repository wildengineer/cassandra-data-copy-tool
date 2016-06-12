package wildengineer.cassandra.data.copy;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.*;
import static wildengineer.cassandra.data.copy.TestUtil.CASSANDRA;
import static wildengineer.cassandra.data.copy.TestUtil.LASTNAME;
import static wildengineer.cassandra.data.copy.TestUtil.LOCALHOST;
import static wildengineer.cassandra.data.copy.TestUtil.TEST_KEYSPACE;
import static wildengineer.cassandra.data.copy.TestUtil.USERS;
import static wildengineer.cassandra.data.copy.TestUtil.VERIFIED_USERS;

import java.util.List;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Sets;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(value = "TestSchemaAndData.cql", keyspace = "test_keyspace")
@EmbeddedCassandra
public class TableDataCopierTest {

	private Session sourceSession;
	private Session sinkSession;

	private CassandraTemplate sourceCassandraTemplate;
	private CassandraTemplate sinkCassandraTemplate;

	@Before
	public void setup() {
		sourceSession = createNewLocalhostSession();
		sourceCassandraTemplate = new CassandraTemplate(sourceSession);
		sinkSession = createNewLocalhostSession();
		sinkCassandraTemplate = new CassandraTemplate(sinkSession);
	}

	@Test
	public void testCopyFromUserToVerifiedUser() throws Exception {

		TableDataCopier tableDataCopier	= new TableDataCopier(sourceSession, sinkSession, new TuningParams());
		tableDataCopier.copy(USERS, VERIFIED_USERS);


	}

	@Test
	public void testCopyFromUserToVerifiedUserWithIgnore() throws Exception {

		TableDataCopier tableDataCopier	= new TableDataCopier(sourceSession, sinkSession, new TuningParams());
		tableDataCopier.copy(USERS, VERIFIED_USERS, Sets.newHashSet(LASTNAME));

		List<UserEntity> sourceList
				= sourceCassandraTemplate.select(select().from(TEST_KEYSPACE, USERS), UserEntity.class);
		sourceList.forEach((user) -> {
			Select selectByEmail = select().from(TEST_KEYSPACE, VERIFIED_USERS);
			selectByEmail.where(eq("email", user.getEmail()));
			List<UserEntity> copiedUsers = sinkCassandraTemplate.select(selectByEmail, UserEntity.class);
			assertFalse(copiedUsers.isEmpty());
			UserEntity copiedUser = copiedUsers.get(0);
			assertNotNull(copiedUser);
			assertEquals(user.getEmail(), copiedUser.getEmail());
			assertEquals(user.getFirstname(), copiedUser.getFirstname());
			assertNotEquals(user.getLastname(), copiedUser.getLastname());
			assertNull(copiedUser.getLastname());
			assertEquals(user.getAge(), copiedUser.getAge());
			assertEquals(user.getCity(), copiedUser.getCity());
		});
	}

	@Test
	public void testBatchRate() throws Exception {

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		TuningParams tuningParams = new TuningParams();
		tuningParams.setBatchSize(1);
		tuningParams.setBatchesPerSecond(1);

		TableDataCopier tableDataCopier
				= new TableDataCopier(createNewLocalhostSession(), createNewLocalhostSession(), tuningParams);

		tableDataCopier.copy("users", "verified_users");

		stopWatch.stop();

		//time should be greater than (count - 1) seconds
		assertTrue(stopWatch.getLastTaskTimeMillis() > 2000);
	}

	private static Session createNewLocalhostSession() {
		Cluster cluster = Cluster.builder()
				.addContactPoints(LOCALHOST)
				.withCredentials(CASSANDRA, CASSANDRA)
				.withPort(9142)
				.build();
		return cluster.connect(TEST_KEYSPACE);
	}
}

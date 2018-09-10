package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.collect.Sets;
import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import java.util.HashSet;

import static org.junit.Assert.assertTrue;
import static wildengineer.cassandra.data.copy.TestUtil.*;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({CassandraUnitTestExecutionListener.class})
@CassandraDataSet(value = "TestSchemaAndData.cql", keyspace = "test_keyspace")
@EmbeddedCassandra
public class TableDataCopierTest {

    private Session sourceSession;
    private Session destinationSession;

//    private CassandraTemplate sourceCassandraTemplate;
//    private CassandraTemplate destinationCassandraTemplate;
    private KeyspaceMetadata sourceKeyspaceMetadata;

    @Before
    public void setup() {
//        sourceSession = createNewLocalhostSession();
//        sourceCassandraTemplate = new CassandraTemplate(sourceSession);
//        destinationSession = createNewLocalhostSession();
//        destinationCassandraTemplate = new CassandraTemplate(destinationSession);
//        sourceKeyspaceMetadata = sourceCassandraTemplate.getSession().getCluster().getMetadata().getKeyspace("test_keyspace");

    }

    @Test
    public void testCopyFromUserToVerifiedUser() throws Exception {

        TableDataCopier tableDataCopier = new TableDataCopier(sourceSession, destinationSession, sourceKeyspaceMetadata, new TuningParams());
        tableDataCopier.copy(USERS, VERIFIED_USERS, new HashSet<>());


    }

    @Test
    public void testCopyFromUserToVerifiedUserWithIgnore() throws Exception {

        TableDataCopier tableDataCopier = new TableDataCopier(sourceSession, destinationSession, sourceKeyspaceMetadata, new TuningParams());
        tableDataCopier.copy(USERS, VERIFIED_USERS, Sets.newHashSet(LASTNAME));
//
//        List<UserEntity> sourceList
//                = sourceCassandraTemplate.select(select().from(TEST_KEYSPACE, USERS), UserEntity.class);
//        sourceList.forEach((user) -> {
//            Select selectByEmail = select().from(TEST_KEYSPACE, VERIFIED_USERS);
//            selectByEmail.where(eq("email", user.getEmail()));
//            List<UserEntity> copiedUsers = destinationCassandraTemplate.select(selectByEmail, UserEntity.class);
//            assertFalse(copiedUsers.isEmpty());
//            UserEntity copiedUser = copiedUsers.get(0);
//            assertNotNull(copiedUser);
//            assertEquals(user.getEmail(), copiedUser.getEmail());
//            assertEquals(user.getFirstname(), copiedUser.getFirstname());
//            assertNotEquals(user.getLastname(), copiedUser.getLastname());
//            assertNull(copiedUser.getLastname());
//            assertEquals(user.getAge(), copiedUser.getAge());
//            assertEquals(user.getCity(), copiedUser.getCity());
//        });
    }

    @Test
    public void testBatchRate() throws Exception {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        TuningParams tuningParams = new TuningParams();
        tuningParams.setBatchSize(1);
        tuningParams.setBatchesPerSecond(1);

        TableDataCopier tableDataCopier
                = new TableDataCopier(createNewLocalhostSession(), createNewLocalhostSession(), sourceKeyspaceMetadata, tuningParams);

        tableDataCopier.copy("users", "verified_users", new HashSet<>());

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

package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.querybuilder.Select;
import org.springframework.data.cassandra.core.CassandraTemplate;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.*;

/**
 * Created by wildengineer on 6/12/16.
 */
public class TestUtil {

	public static final String LOCALHOST = "127.0.0.1";
	public static final String CASSANDRA = "cassandra";
	public static final String TEST_KEYSPACE = "test_keyspace";
	public static final String USERS = "users";
	public static final String VERIFIED_USERS = "verified_users";
	public static final String LASTNAME = "lastname";

	public static void verifyUsers(CassandraTemplate sourceCassandraTemplate, CassandraTemplate destinationCassandraTemplate) {
		List<UserEntity> sourceList
				= sourceCassandraTemplate.select(select().from(TEST_KEYSPACE, USERS), UserEntity.class);
		sourceList.forEach((user) -> {
			Select selectByEmail = select().from(TEST_KEYSPACE, VERIFIED_USERS);
			selectByEmail.where(eq("email", user.getEmail()));
			List<UserEntity> copiedUsers = destinationCassandraTemplate.select(selectByEmail, UserEntity.class);
			assertFalse(copiedUsers.isEmpty());
			UserEntity copiedUser = copiedUsers.get(0);
			assertNotNull(copiedUser);
			assertEquals(user.getEmail(), copiedUser.getEmail());
			assertEquals(user.getFirstname(), copiedUser.getFirstname());
			assertEquals(user.getLastname(), copiedUser.getLastname());
			assertEquals(user.getAge(), copiedUser.getAge());
			assertEquals(user.getCity(), copiedUser.getCity());
			assertEquals("Source user not equal to copied user", user, copiedUser);
		});
	}
}

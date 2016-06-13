package wildengineer.cassandra.data.copy;

/**
 * Created by mgroves on 5/28/16.
 */
public class CassandraProperties {

	//TODO: Add validation to this base class
	//TODO: Add defaults with static fields
	//TODO: Add retry policy

	public static final String DEFAULT_CONTACT_POINTS = "127.0.0.1";
	public static final int DEFAULT_PORT = 9142;
	public static final String DEFAULT_USERNAME = "cassandra";
	public static final String DEFAULT_PASSWORD = "cassandra";

	private String contactPoints = DEFAULT_CONTACT_POINTS;
	private Integer port = DEFAULT_PORT;
	private String keyspace;
	private String username = DEFAULT_USERNAME;
	private String password = DEFAULT_PASSWORD;

	public String getContactPoints() {
		return contactPoints;
	}

	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}

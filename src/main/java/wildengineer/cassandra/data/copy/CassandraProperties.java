package wildengineer.cassandra.data.copy;

/**
 * Created by mgroves on 5/28/16.
 */
public class CassandraProperties {

	//TODO: Add validation to this base class
	//TODO: Add defaults with static fields
	//TODO: Add retry policy

	private String contactPoints;
	private Integer port;
	private String keyspace;
	private String username;
	private String password;
	private int fetchSize;
	private int consistencyLevelCode;
	private int statementRetryCount;

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

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public int getConsistencyLevelCode() {
		return consistencyLevelCode;
	}

	public void setConsistencyLevelCode(int consistencyLevelCode) {
		this.consistencyLevelCode = consistencyLevelCode;
	}

	public int getStatementRetryCount() {
		return statementRetryCount;
	}

	public void setStatementRetryCount(int statementRetryCount) {
		this.statementRetryCount = statementRetryCount;
	}
}

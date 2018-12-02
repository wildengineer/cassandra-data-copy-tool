package wildengineer.cassandra.data.copy;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by mgroves on 3/28/16.
 */
@Component
@ConfigurationProperties(prefix = "copy")
public class CopyProperties extends TuningParams {

	//TODO: Add validation
	
	private String tables;
	private String ignoreColumns;

	public String getTables() {
		return tables;
	}

	public void setTables(String tables) {
		this.tables = tables;
	}

	public String getIgnoreColumns() {
		return ignoreColumns;
	}

	public void setIgnoreColumns(String ignoreColumns) {
		this.ignoreColumns = ignoreColumns;
	}
}


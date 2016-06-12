package wildengineer.cassandra.data.copy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * CommandLineRunner implementation for copying a keyspace from one cassandra database to another.
 *
 * This tool assumes that the source and sink keyspaces are identical.
 *
 * Created by mgroves on 3/27/16.
 */
@Component
public class CopyRunner implements CommandLineRunner {

	private Logger LOGGER = LogManager.getLogger(CopyRunner.class);

	public static final String TABLE_MAPPING_DELIMETER = "=>";

	private final CopyProperties copyProperties;
	private final Map<String, Set<String>> ignoreMap = new HashMap<>();
	private final Session sourceSession;
	private final Session sinkSession;

	@Autowired
	public CopyRunner(CopyProperties copyProperties, @Qualifier("sourceSession") Session sourceSession,
			@Qualifier("sinkSession") Session sinkSession) {
		this.copyProperties = copyProperties;
		this.sourceSession = sourceSession;
		this.sinkSession = sinkSession;

		//initialize ignoreMap
		if (!StringUtils.isEmpty(copyProperties.getIgnoreColumns())) {
			String[] columnDefinitions = copyProperties.getIgnoreColumns().split(",");
			Arrays.asList(columnDefinitions).forEach(cd -> {
				String[] tableAndProperty = cd.split("\\.");
				String table = tableAndProperty[0].toLowerCase();
				String property = tableAndProperty[1].toLowerCase();
				if (!ignoreMap.containsKey(table)) {
					ignoreMap.put(table, new HashSet<>());
				}
				ignoreMap.get(table).add(property);
			});
		}
	}

	@Override
	public void run(String... args) throws Exception {

		LOGGER.info("Copying data for tables: {}", copyProperties.getTables());

		List<String[]> tables = Arrays.asList(copyProperties.getTables().split(","))
			.stream().map(s -> s.trim().toLowerCase())
			.map(s -> s.split(TABLE_MAPPING_DELIMETER))
			.collect(Collectors.toList());

		TableDataCopier tableDataCopier = new TableDataCopier(sourceSession, sinkSession, copyProperties);
		for (String[] table : tables) {
			Set<String> ignoreSet = ignoreMap.get(table[0]);
			if (table.length == 1) {
				tableDataCopier.copy(table[0], table[0], ignoreSet);
			} else if (table.length == 2) {
				tableDataCopier.copy(table[0], table[1], ignoreSet);
			} else {
				throw new IllegalArgumentException("Table configuration is invalid.");
			}
		}
	}

}

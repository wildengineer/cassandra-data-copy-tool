package wildengineer.cassandra.data.copy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

/**
 * CommandLineRunner implementation for copying a keyspace from one cassandra database to another.
 * <p>
 * This tool assumes that the source and destination keyspaces are identical.
 * <p>
 * Created by wildengineer on 3/27/16.
 */
@Component
public class CopyRunner implements CommandLineRunner {

    private Logger LOGGER = LogManager.getLogger(CopyRunner.class);

    public static final String TABLE_MAPPING_DELIMETER = "=>";

    private final CopyProperties copyProperties;
    private final Map<String, Set<String>> ignoreMap = new HashMap<>();
    private final Session sourceSession;
    private final Session destinationSession;

    @Autowired
    public CopyRunner(CopyProperties copyProperties, @Qualifier("sourceSession") Session sourceSession,
            @Qualifier("destinationSession") Session destinationSession) {
        this.copyProperties = copyProperties;
        this.sourceSession = sourceSession;
        this.destinationSession = destinationSession;

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

        LOGGER.debug("Copying data for tables: {}", copyProperties.getTables());

        List<Pair<String, String>> tablePairs = getTablePairs();

        if (LOGGER.isDebugEnabled()) {
            tablePairs.forEach(p -> LOGGER.debug("Source: {} Destination: {}", p.getLeft(), p.getRight()));
        }

        TableDataCopier tableDataCopier = new TableDataCopier(sourceSession, destinationSession, copyProperties);
        for (Pair<String, String> tablePair : tablePairs) {
            Set<String> ignoreSet = ignoreMap.get(tablePair.getLeft());
            tableDataCopier.copy(tablePair.getLeft(), tablePair.getRight(), ignoreSet);
        }
        LOGGER.debug("Finished data for tables: {}", copyProperties.getTables());
    }

    private List<Pair<String, String>> getTablePairs() {
        List<String[]> tables = Arrays.stream(copyProperties.getTables().split(TABLE_MAPPING_DELIMETER))
                .map(s -> s.split(","))
                .collect(Collectors.toList());
        int size = tables.size();
        if (size == 1) {
            return Arrays.stream(tables.get(0)).map(t -> Pair.of(t, t)).collect(Collectors.toList());
        } else if (size == 2) {
            String[] sourceTables = tables.get(0);
            String[] sinkTables = tables.get(1);
            Preconditions.checkState(sourceTables.length == sinkTables.length);
            return IntStream.range(0, Math.min(sourceTables.length, sinkTables.length))
                    .mapToObj(i -> Pair.of(sourceTables[i], sinkTables[i]))
                    .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("copy.tables is formatted incorrectly.");
        }
    }

}

package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wildengineer on 5/28/16.
 */
public class TableDataCopier {

    private static Logger LOGGER = LogManager.getLogger(TableDataCopier.class);

    private final Session sourceSession;
    private final Session destinationSession;
    private final TuningParams tuningParams;

    private final String INSERT_STATEMENT = "insert into %s (%s) values (%s)";

    @Autowired
    public TableDataCopier(Session sourceSession, Session destinationSession, TuningParams tuningParams) {
        this.sourceSession = sourceSession;
        this.destinationSession = destinationSession;
        this.tuningParams = tuningParams;
    }

    public void copy(String fromTable, String toTable) throws Exception {
        copy(fromTable, toTable, null);
    }

    @SuppressWarnings("UnstableApiUsage")
    public void copy(String fromTable, String toTable, Set<String> ignoreColumns) throws Exception {

        LOGGER.info("Copying data from table {} to table {}", fromTable, toTable);

        Select selectFromCustomer = QueryBuilder.select().from(fromTable);
        ResultSet rs = sourceSession.execute(selectFromCustomer);
        List<List<?>> rowsToIngest = new ArrayList<>();
        List<ColumnDefinitions.Definition> columnDefinitions = null;
        PreparedStatement insertStatement = null;
        RateLimiter rateLimiter = RateLimiter.create(tuningParams.getBatchSize() * tuningParams.getBatchesPerSecond());

        int copyCount = 0;

        for (Row row : rs) {

            int availableWithoutFetching = rs.getAvailableWithoutFetching();
            if (availableWithoutFetching <= tuningParams.getQueryPageSize() && !rs.isFullyFetched()) {
                rs.fetchMoreResults(); // async fetch
            }

            if (columnDefinitions == null) {
                columnDefinitions = row.getColumnDefinitions().asList().stream()
                        .filter(cd -> ignoreColumns == null || !ignoreColumns.contains(cd.getName().toLowerCase()))
                        .collect(Collectors.toList());
                insertStatement = buildInsertStatement(toTable,
                        columnDefinitions.stream().map(ColumnDefinitions.Definition::getName)
                                .collect(Collectors.toList()));

                LOGGER.debug("Built insert statement: {}", insertStatement.toString());
            }

            rowsToIngest.add(columnDefinitions.stream().map(cd -> getValue(row, cd)).collect(Collectors.toList()));

            if (rowsToIngest.size() >= tuningParams.getBatchSize()) {
                List<List<?>> copyOfRowsToIngest = new ArrayList<>(rowsToIngest);
                rowsToIngest.clear();
                LOGGER.info("Ingesting {} rows to destination", copyOfRowsToIngest.size());
                ingest(insertStatement, rowIterator(copyOfRowsToIngest), rateLimiter);
                copyCount += copyOfRowsToIngest.size();
            }
        }
        ingest(insertStatement, rowIterator(rowsToIngest), rateLimiter);
        copyCount += rowsToIngest.size();

        LOGGER.info("Finished copying {} rows from table {} to table {}", copyCount, fromTable, toTable);
    }

    private void ingest(PreparedStatement preparedStatement, Iterator<Object[]> rowIterator, RateLimiter rateLimiter) {
        while (rowIterator.hasNext()) {
            rateLimiter.acquire();
            destinationSession.executeAsync(preparedStatement.bind(rowIterator.next()));
        }
    }

    private Iterator<Object[]> rowIterator(List<List<?>> rowsToIngest) {
        return new Iterator<Object[]>() {
            Iterator<List<?>> i = rowsToIngest.iterator();

            public Object[] next() {
                return this.i.next().toArray();
            }

            public boolean hasNext() {
                return this.i.hasNext();
            }
        };
    }

    private Object getValue(Row row, ColumnDefinitions.Definition definition) {
        DataType.Name dataType = definition.getType().getName();
        Object value;
        String name = definition.getName();
        switch (dataType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                value = row.getString(name);
                break;
            case BIGINT:
            case COUNTER:
                value = row.getLong(name);
                break;
            case BLOB:
            case CUSTOM:
                value = row.getBytes(name);
                break;
            case BOOLEAN:
                value = row.getBool(name);
                break;
            case DECIMAL:
                value = row.getDecimal(name);
                break;
            case DOUBLE:
                value = row.getDouble(name);
                break;
            case FLOAT:
                value = row.getFloat(name);
                break;
            case INET:
                value = row.getInet(name);
                break;
            case INT:
                value = row.getInt(name);
                break;
            case LIST:
                value = row.getList(name, Object.class);
                break;
            case SET:
                value = row.getSet(name, Object.class);
                break;
            case MAP:
                value = row.getMap(name, String.class, Object.class);
                break;
            case TIMESTAMP:
                value = row.getTimestamp(name);
                break;
            case TIMEUUID:
            case UUID:
                value = row.getUUID(name);
                break;
            case TUPLE:
                value = row.getTupleValue(name);
                break;
            case UDT:
                value = row.getUDTValue(name);
                break;
            case VARINT:
                value = row.getVarint(name);
                break;
            case TINYINT:
                value = row.getByte(name);
                break;
            default:
                throw new IllegalStateException("Encountered unknown Cassandra DataType (" + dataType + ")");
        }
        return value;
    }

    private PreparedStatement buildInsertStatement(String table, List<String> fields) {

        StringBuilder fieldSB = new StringBuilder();
        StringBuilder placeholderSB = new StringBuilder();
        int fieldListCount = fields.size();
        for (String f : fields) {
            fieldSB.append(f);
            placeholderSB.append("?");
            if (fieldListCount > 1) {
                fieldSB.append(",");
                placeholderSB.append(",");
            }
            fieldListCount--;
        }
        String statementString = String.format(INSERT_STATEMENT, table, fieldSB.toString(), placeholderSB.toString());
        return destinationSession.prepare(statementString);
    }
}

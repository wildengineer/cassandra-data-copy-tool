package wildengineer.cassandra.data.copy;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.CachedPreparedStatementCreator;
import org.springframework.cassandra.core.RowIterator;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Created by mgroves on 5/28/16.
 */
public class TableDataCopier {

    private static Logger LOGGER = LogManager.getLogger(TableDataCopier.class);
    private static final int THREADS = 4;
    private final Session sourceSession;
    private final Session destinationSession;
    private final TuningParams tuningParams;
    private final KeyspaceMetadata sourceKeyspaceMetadata;

    private LocalDateTime currentTime = LocalDateTime.now();
    private Long secondsStart = System.currentTimeMillis();
    private List<String> sourceFields;

    public void logStats(Long copyCount) {
        Long secondsNow = System.currentTimeMillis();
        Long since = secondsNow - secondsStart;
        long seconds = since / 1000; // Maybe no need to divide if the input is in seconds
        LocalTime timeOfDay = LocalTime.ofSecondOfDay(seconds);
        String time = timeOfDay.toString();
        LOGGER.info("TotalCount: {}, Total time: {}, StartTime: {}", copyCount, time, currentTime.toString());
    }

    private final String INSERT_STATEMENT = "insert into %s (%s) values (%s)";

    private long copyCount = 0;
    private final ExecutorService executor;

    @Autowired
    public TableDataCopier(Session sourceSession, Session destinationSession, KeyspaceMetadata sourceKeyspaceMetadata, TuningParams tuningParams) {
        this.sourceSession = sourceSession;
        this.destinationSession = destinationSession;
        this.tuningParams = tuningParams;
        this.sourceKeyspaceMetadata = sourceKeyspaceMetadata;

        executor = MoreExecutors.getExitingExecutorService(
                (ThreadPoolExecutor) Executors.newFixedThreadPool(THREADS));

    }

    private void copy(String fromTable, String toTable) throws Exception {
        TableMetadata sourceTableMetadata = sourceKeyspaceMetadata.getTable(fromTable);
        sourceFields = sourceTableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        String insertStatement = buildInsertStatement(toTable, sourceFields);
        copy(fromTable, toTable, null);

        System.out.println(insertStatement);
    }


    private AsyncFunction<ResultSet, ResultSet> iterate(PreparedStatement preparedStatement, TuningParams tuningParams, final int page) {
        RateLimiter rateLimiter = RateLimiter.create(tuningParams.getBatchSize() * tuningParams.getBatchesPerSecond());
        List<List<?>> rowsToIngest = new ArrayList<>();
        return rs -> {
            List<ColumnDefinitions.Definition> columnDefinitions = null;
            // How far we can go without triggering the blocking fetch:
            int remainingInPage = rs.getAvailableWithoutFetching();

            System.out.printf("Starting page %d (%d rows)%n", page, remainingInPage);

            for (Row row : rs) {
                rowsToIngest.add(sourceFields);

                if (columnDefinitions == null) {
                    columnDefinitions = new ArrayList<>(row.getColumnDefinitions().asList());
                }

                rowsToIngest.add(columnDefinitions.stream().map(cd -> getValue(row, cd)).collect(Collectors.toList()));

                if (rowsToIngest.size() >= tuningParams.getBatchSize()) {
                    List<List<?>> copyOfRowsToIngest = new ArrayList<>(rowsToIngest);
                    rowsToIngest.clear();
                    copyCount += copyOfRowsToIngest.size();
                    LOGGER.debug("Ingesting {} rows to destination, Total Copy Count: {}", copyOfRowsToIngest.size(), copyCount);
                    ingest(preparedStatement, rowIterator(copyOfRowsToIngest), rateLimiter);
                    logStats(copyCount);
                }
                if (--remainingInPage == 0)
                    break;
            }
            System.out.printf("Done page %d%n", page);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                System.out.println("Done iterating");
                return Futures.immediateFuture(rs);
            } else {
                int currentPage = page + 1;
                System.out.printf("Fetching More More records using Page: %d%n", currentPage);
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transform(future, iterate(preparedStatement, tuningParams, currentPage));
            }
        };
    }

    public void copy(String fromTable, String toTable, Set<String> ignoreColumns) throws Exception {
        BoundStatement insertBoundStatement = createInsertBoundStatement(fromTable, toTable);
        PreparedStatement preparedStatement = createPreparedStatement(fromTable, toTable);

        LOGGER.debug("Copying data from table {} to table {}", fromTable, toTable);

        Statement statement = QueryBuilder.select().from(fromTable);
        statement.setFetchSize(3000);
        ResultSetFuture resultSetFuture = sourceSession.executeAsync(statement);

        Futures.transform(resultSetFuture, iterate(preparedStatement, tuningParams,1), executor);
    }

    private BoundStatement createInsertBoundStatement(String fromTable, String toTable) {
        TableMetadata sourceTableMetadata = sourceKeyspaceMetadata.getTable(fromTable);
        sourceFields = sourceTableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        String insertStatement = buildInsertStatement(toTable, sourceFields);
        PreparedStatement insertPreparedStatement = destinationSession.prepare(insertStatement);
        return new BoundStatement(insertPreparedStatement);
    }

    private PreparedStatement createPreparedStatement(String fromTable, String toTable) {
        TableMetadata sourceTableMetadata = sourceKeyspaceMetadata.getTable(fromTable);
        sourceFields = sourceTableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        String insertStatement = buildInsertStatement(toTable, sourceFields);
        return destinationSession.prepare(insertStatement);
    }

    private void ingest(PreparedStatement preparedStatement, RowIterator rowIterator, RateLimiter rateLimiter) {
        while (rowIterator.hasNext()) {
            rateLimiter.acquire();
            destinationSession.executeAsync(preparedStatement.bind(rowIterator.next()));
        }
    }

    private RowIterator rowIterator(List<List<?>> rowsToIngest) {
        return new RowIterator() {
            Iterator<List<?>> i = rowsToIngest.iterator();

            public Object[] next() {
                return ((List) this.i.next()).toArray();
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
                value = row.getDate(name);
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
            default:
                throw new IllegalStateException("Encountered unknown Cassandra DataType.");
        }
        return value;
    }

    private String buildInsertStatement(String table, List<String> fields) {

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


        return String.format(INSERT_STATEMENT, table, fieldSB.toString(), placeholderSB.toString());
    }
}

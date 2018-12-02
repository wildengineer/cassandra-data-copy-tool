package wildengineer.cassandra.data.copy;

/**
 * Created by wildengineer on 5/29/16.
 */
public class TuningParams {

	private static final int DEFAULT_BATCH_SIZE = 20000; //SIZE OF BATCH TO COPY AT ONCE
	private static final int DEFAULT_QUERY_PAGE_SIZE = 1000; //FETCH QUERY RESULTS IN PAGE SIZE 1000
	private static final int DEFAULT_BATCHES_PER_SECOND = 1; //ONE SECOND

	private int batchSize = DEFAULT_BATCH_SIZE;
	private int queryPageSize = DEFAULT_QUERY_PAGE_SIZE;
	private int batchesPerSecond = DEFAULT_BATCHES_PER_SECOND;

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getQueryPageSize() {
		return queryPageSize;
	}

	public void setQueryPageSize(int queryPageSize) {
		this.queryPageSize = queryPageSize;
	}

	public int getBatchesPerSecond() {
		return batchesPerSecond;
	}

	public void setBatchesPerSecond(int batchesPerSecond) {
		this.batchesPerSecond = batchesPerSecond;
	}
}

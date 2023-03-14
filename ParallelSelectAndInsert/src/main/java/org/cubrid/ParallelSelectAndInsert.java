package org.cubrid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParallelSelectAndInsert {
	private static final Logger LOGGER = Logger.getLogger(ParallelSelectAndInsert.class.getName());

	private int numThreads;
	private Connection sourceConnection;
	private List<Connection> targetConnectionList;
	private ExecutorService executorService;

	private long rowCount;
	private long rowCountPerThread;
	private long rowCountRemain;

	public ParallelSelectAndInsert(int numThreads) {
		this.numThreads = (numThreads > 0) ? numThreads : 1;
	}

	public void start(String sourceTableName, String targetTableName) {
		try {
			startInternal(sourceTableName, targetTableName);
		} catch (Exception e) {
			if (!executorService.isTerminated()) {
				executorService.shutdownNow();
			}

			closeConnectionswithRollback();
		}
	}

	public void startInternal(String sourceTableName, String targetTableName) {
		sourceConnection = CUBRIDConnectionManager.getSourceConnection();

		String countQuery = CUBRIDConnectionManager.getCountQuery(sourceTableName);
		try (PreparedStatement sourceStatement = sourceConnection.prepareStatement(countQuery)) {
			try (ResultSet resultSet = sourceStatement.executeQuery()) {
				if (resultSet.next()) {
					rowCount = resultSet.getLong(1);
					rowCountPerThread = rowCount / numThreads;
					rowCountRemain = rowCount % numThreads;
				} else {
					LOGGER.log(Level.WARNING, "No results returned from the query executed on the source connection");
					closeConnectionswithRollback();
					return;
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to execute query for source connection", e);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to create prepared statement for source connection", e);
		}

		targetConnectionList = new ArrayList<Connection>(numThreads);
		for (int i = 0; i < numThreads; i++) {
			targetConnectionList.add(CUBRIDConnectionManager.getTargetConnection());
		}

		List<CopyTask> copyTaskList = new ArrayList<>();
		copyTaskList.add(new CopyTask(sourceConnection, targetConnectionList.get(0), sourceTableName, 0,
				rowCountPerThread + rowCountRemain, targetTableName));
		for (int i = 1; i < numThreads; i++) {
			copyTaskList.add(new CopyTask(sourceConnection, targetConnectionList.get(i), sourceTableName,
					rowCountPerThread * i, rowCountPerThread, targetTableName));
		}

		executorService = Executors.newFixedThreadPool(numThreads);

		List<Future<Long>> copyFutureList = null;
		try {
			copyFutureList = executorService.invokeAll(copyTaskList);

			long insertCount = 0;

			for (Future<Long> copyFuture : copyFutureList) {
				insertCount += copyFuture.get().longValue();
				LOGGER.log(Level.INFO, "Finished processing " + insertCount + " rows");
			}
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, "Interrupted while waiting for copy tasks to complete", e);
		} catch (ExecutionException e) {
			LOGGER.log(Level.SEVERE, "Execution error while waiting for copy task to complete", e);
		}

		executorService.shutdown();
		try {
			executorService.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, "Interrupted while waiting for executor services to terminate", e);
			Thread.currentThread().interrupt();
		}

		closeConnectionswithCommit();
	}

	private void closeConnectionswithCommit() {
		for (Connection targetConnection : targetConnectionList) {
			if (targetConnection != null) {
				try {
					targetConnection.commit();
					LOGGER.log(Level.INFO, "Committed transaction");
					targetConnection.close();
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE, "Failed to commit and close target connection", e);
					closeConnectionswithRollback();
				}
			}
		}

		if (sourceConnection != null) {
			try {
				sourceConnection.commit();
				LOGGER.log(Level.INFO, "Committed transaction");
				sourceConnection.close();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to commit and close source connection", e);
				closeConnectionswithRollback();
			}
		}
	}

	private void closeConnectionswithRollback() {
		for (Connection targetConnection : targetConnectionList) {
			if (targetConnection != null) {
				try {
					targetConnection.rollback();
					targetConnection.close();
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE, "Failed to rollback and close target connection", e);
				}
			}
		}

		if (sourceConnection != null) {
			try {
				sourceConnection.rollback();
				sourceConnection.close();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to rollback and close source connection", e);
			}
		}
	}
}

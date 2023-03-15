package org.cubrid;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CopyTask implements Callable<Long> {
	private static final Logger LOGGER = Logger.getLogger(CopyTask.class.getName());

	private String currentThreadName;

	private final Connection sourceConnection;
	private final Connection targetConnection;
	private final String sourceTableName;
	private final String sourceIndexColumnName;
	private final long offset;
	private final long rowCount;
	private final String targetTableName;

	private int batchCount;
	private long addBatchCount;
	private long executeBatchCount;

	public CopyTask(Connection sourceConnection, Connection targetConnection, String sourceTableName,
			String sourceIndexColumnName, long offset, long rowCount, String targetTableName) {
		this.sourceConnection = sourceConnection;
		this.targetConnection = targetConnection;
		this.sourceTableName = sourceTableName;
		this.sourceIndexColumnName = sourceIndexColumnName;
		this.offset = offset;
		this.rowCount = rowCount;
		this.targetTableName = targetTableName;

		this.batchCount = ParallelSelectAndInsert.DEFAULT_BATCH_COUNT;
		this.addBatchCount = 0;
		this.executeBatchCount = 0;
	}

	@Override
	public Long call() {
		this.currentThreadName = Thread.currentThread().getName();
		LOGGER.log(Level.INFO, String.format("[%s] Starting copy task thread.", currentThreadName));

		String selectQuery = null;
		if (sourceIndexColumnName != null) {
			selectQuery = CUBRIDConnectionManager.getSelectQueryUsingIndex(sourceTableName, sourceIndexColumnName);
		} else {
			selectQuery = CUBRIDConnectionManager.getSelectQuery(sourceTableName);
		}

		try (PreparedStatement sourceStatement = sourceConnection.prepareStatement(selectQuery)) {
			sourceStatement.setLong(1, offset);
			sourceStatement.setLong(2, rowCount);

			try (ResultSet resultSet = sourceStatement.executeQuery()) {
				ResultSetMetaData metadata = resultSet.getMetaData();
				int columnCount = metadata.getColumnCount();

				String insertQuery = CUBRIDConnectionManager.getInsertQuery(targetTableName, columnCount);

				try (PreparedStatement targetStatement = targetConnection.prepareStatement(insertQuery)) {
					try {
						while (resultSet.next()) {
							for (int i = 1; i <= columnCount; i++) {
								targetStatement.setObject(i, resultSet.getObject(i));
							}
							targetStatement.addBatch();
							addBatchCount++;

							if (addBatchCount == batchCount) {
								try {
									targetStatement.executeBatch();
									LOGGER.log(Level.INFO,
											String.format("[%s] Inserted %d rows", currentThreadName, addBatchCount));

									executeBatchCount += addBatchCount;
									addBatchCount = 0;
								} catch (BatchUpdateException e) {
									LOGGER.log(Level.SEVERE,
											String.format("[%s] Failed to execute batch for target connection",
													currentThreadName),
											e);
									return new Long(-1);
								}
							}
						}
					} catch (SQLException e) {
						LOGGER.log(Level.SEVERE,
								String.format("[%s] Failed to get data for source connection", currentThreadName), e);
						Thread.currentThread().interrupt();
						return new Long(-1);
					}

					if (addBatchCount != 0) {
						try {
							targetStatement.executeBatch();
							LOGGER.log(Level.INFO,
									String.format("[%s] Inserted %d rows", currentThreadName, addBatchCount));

							executeBatchCount += addBatchCount;
						} catch (BatchUpdateException e) {
							LOGGER.log(Level.SEVERE, String.format("[%s] Failed to execute batch for target connection",
									currentThreadName), e);
							return new Long(-1);
						}
					}
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE, String.format(
							"[%s] Failed to create prepared statement for target connection", currentThreadName), e);
					return new Long(-1);
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE,
						String.format("[%s] Failed to execute query for source connection", currentThreadName), e);
				return new Long(-1);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE,
					String.format("[%s] Failed to create prepared statement for source connection", currentThreadName),
					e);
			return new Long(-1);
		}

		return new Long(executeBatchCount);
	}
}

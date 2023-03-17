package org.cubrid;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class CopyTask implements Callable<Void> {
	private int threadNum;
	private Connection sourceConnection;
	private String sourceTableName;
	private String sourceIndexFirstColumnName;
	private long offset;
	private long rowCount;
	private Connection destinationConnection;
	private String destinationTableName;
	private int batchCount;
	private ProgressBarTask progressBar;

	private long addBatchCount;
	private long executeBatchCount;

	public CopyTask(CopyTaskInfo copyTaskInfo) {
		this.threadNum = copyTaskInfo.getThreadNum();
		this.sourceConnection = copyTaskInfo.getSourceConnection();
		this.sourceTableName = copyTaskInfo.getSourceTableName();
		this.sourceIndexFirstColumnName = copyTaskInfo.getSourceIndexFirstColumnName();
		this.offset = copyTaskInfo.getOffset();
		this.rowCount = copyTaskInfo.getRowCount();
		this.destinationConnection = copyTaskInfo.getDestinationConnection();
		this.destinationTableName = copyTaskInfo.getDestinationTableName();
		this.batchCount = copyTaskInfo.getBatchCount();
		this.progressBar = copyTaskInfo.getProgressBar();

		assert (sourceConnection != null);
		assert (sourceTableName != null);
		assert (offset > 0);
		assert (rowCount > 0);
		assert (destinationConnection != null);
		assert (destinationTableName != null);
		assert (batchCount > 0);

		this.addBatchCount = 0;
		this.executeBatchCount = 0;
	}

	@Override
	public Void call() throws SQLException {
		String fetchSourceRecordsQuery = ConnectionManager.getFetchSourceRecordsQuery(sourceTableName,
				sourceIndexFirstColumnName);

		try (PreparedStatement sourceStatement = sourceConnection.prepareStatement(fetchSourceRecordsQuery)) {
			sourceStatement.setLong(1, offset);
			sourceStatement.setLong(2, rowCount);

			try (ResultSet resultSet = sourceStatement.executeQuery()) {
				ResultSetMetaData metadata = resultSet.getMetaData();
				int columnCount = metadata.getColumnCount();

				String insertRecordToDestinationQuery = ConnectionManager
						.getInsertRecordToDestinationQuery(destinationTableName, columnCount);

				try (PreparedStatement destinationStatement = destinationConnection
						.prepareStatement(insertRecordToDestinationQuery)) {
					while (resultSet.next()) {
						for (int i = 1; i <= columnCount; i++) {
							destinationStatement.setObject(i, resultSet.getObject(i));
						}
						destinationStatement.addBatch();
						addBatchCount++;

						if (addBatchCount == batchCount) {
							destinationStatement.executeBatch();
							executeBatchCount += addBatchCount;

							progressBar.setProgressPerThread(threadNum, executeBatchCount);
							progressBar.addProgressOfMain(addBatchCount);

							addBatchCount = 0;
						}
					}

					if (addBatchCount != 0) {
						destinationStatement.executeBatch();
						executeBatchCount += addBatchCount;

						progressBar.setProgressPerThread(threadNum, executeBatchCount);
						progressBar.addProgressOfMain(addBatchCount);

						addBatchCount = 0;
					}
				} catch (SQLException e) { /* destinationStatement */
					throw e;
				}
			} catch (SQLException e) { /* resultSet */
				throw e;
			}
		} catch (SQLException e) { /* sourceStatement */
			throw e;
		}

		return null;
	}
}
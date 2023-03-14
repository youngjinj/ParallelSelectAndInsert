package org.cubrid;

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

	private final Connection sourceConnection;
	private final Connection targetConnection;
	private final String sourceTableName;
	private final long offset;
	private final long rowCount;
	private final String targetTableName;

	private long insertCount;

	public CopyTask(Connection sourceConnection, Connection targetConnection, String sourceTableName, long offset,
			long rowCount, String targetTableName) {
		this.sourceConnection = sourceConnection;
		this.targetConnection = targetConnection;
		this.sourceTableName = sourceTableName;
		this.offset = offset;
		this.rowCount = rowCount;
		this.targetTableName = targetTableName;

		this.insertCount = 0;
	}

	@Override
	public Long call() {
		// String selectQuery = CUBRIDConnectionManager.getSelectQuery(sourceTableName);

		String sourceColumnName = "c1";
		String selectQuery = CUBRIDConnectionManager.getSelectQueryUsingIndex(sourceTableName, sourceColumnName);

		try (PreparedStatement sourceStatement = sourceConnection.prepareStatement(selectQuery)) {
			sourceStatement.setLong(1, offset);
			sourceStatement.setLong(2, rowCount);

			try (ResultSet resultSet = sourceStatement.executeQuery()) {
				ResultSetMetaData metadata = resultSet.getMetaData();
				int columnCount = metadata.getColumnCount();

				String insertQuery = CUBRIDConnectionManager.getInsertQuery(targetTableName, columnCount);

				try (PreparedStatement targetStatement = targetConnection.prepareStatement(insertQuery)) {
					while (resultSet.next()) {
						for (int i = 1; i <= columnCount; i++) {
							targetStatement.setObject(i, resultSet.getObject(i));
						}
						targetStatement.addBatch();
						insertCount++;

						if (insertCount % 1000 == 0) {
							targetStatement.executeBatch();
						}
					}

					if (insertCount % 1000 != 0) {
						targetStatement.executeBatch();
					}
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE,
							"Failed to create prepared statement or execute batch for target connection", e);
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to execute query for source connection", e);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to create prepared statement for source connection", e);
		}

		return new Long(insertCount);
	}
}

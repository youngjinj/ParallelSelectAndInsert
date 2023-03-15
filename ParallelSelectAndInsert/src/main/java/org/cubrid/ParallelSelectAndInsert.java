package org.cubrid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import cubrid.jdbc.driver.CUBRIDXid;

public class ParallelSelectAndInsert {
	private static final Logger LOGGER = Logger.getLogger(ParallelSelectAndInsert.class.getName());

	public static final int DEFAULT_BATCH_COUNT = 1000;

	private int numThreads;
	private Connection sourceConnection;
	private List<XAConnection> targetXAConnectionList;
	private List<XAResource> targetXAResourceList;
	private List<Xid> targetXidList;
	private List<Connection> targetConnectionList;
	private ExecutorService executorService;

	private long rowCount;
	private long rowCountPerThread;
	private long rowCountRemain;
	private long totalinsertRowCount;

	public ParallelSelectAndInsert(int numThreads) {
		this.numThreads = (numThreads > 0) ? numThreads : 1;
		this.totalinsertRowCount = 0;
	}

	public void start(String sourceTableName, String targetTableName) {
		LOGGER.log(Level.INFO, "Starting Parallel Select and Insert program.");

		try {
			startInternal(sourceTableName, targetTableName);

			if (totalinsertRowCount > 0 && checkXAResource()) {
				System.out.println("step-1");
				allXACommit();
			} else {
				System.out.println("step-2");
				allXARollback();
			}
		} catch (Exception e) {
			if (executorService != null && !executorService.isTerminated()) {
				executorService.shutdownNow();
			}

			System.out.println("step-3");
			allXARollback();
		}

		closeConnections();
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
				}

				if (rowCount == 0) {
					LOGGER.log(Level.WARNING, "No results returned from the query executed on the source connection");
					return;
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to execute query for source connection", e);
				return;
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to create prepared statement for source connection", e);
			return;
		}

		targetXAConnectionList = new ArrayList<XAConnection>(numThreads);
		targetXAResourceList = new ArrayList<XAResource>(numThreads);
		targetXidList = new ArrayList<Xid>(numThreads);
		targetConnectionList = new ArrayList<Connection>(numThreads);
		
		try {
			for (int i = 0; i < numThreads; i++) {
				XAConnection xaConnection = CUBRIDConnectionManager.getTargetXAConnection();
				XAResource xaResource = xaConnection.getXAResource();
				Xid xid = new CUBRIDXid(100, new byte[]{0x01}, new byte[]{(byte) i});
				Connection connection = xaConnection.getConnection();
				connection.setAutoCommit(false);

				targetXAConnectionList.add(xaConnection);
				targetXAResourceList.add(xaResource);
				targetXidList.add(xid);
				targetConnectionList.add(connection);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		try {
			Iterator<XAResource> targetXAResourceIter = targetXAResourceList.iterator();
			Iterator<Xid> targetXidIter = targetXidList.iterator();

			while (targetXAResourceIter.hasNext() && targetXidIter.hasNext()) {
				XAResource targetXAResource = targetXAResourceIter.next();
				Xid targetXid = targetXidIter.next();

				targetXAResource.start(targetXid, XAResource.TMNOFLAGS);
			}
		} catch (XAException e) {
			e.printStackTrace();
		}

		String indexFirstColumnName = getIndexFirstColumnName(sourceTableName);
		List<CopyTask> copyTaskList = new ArrayList<>();
		for (int i = 0; i < (numThreads - 1); i++) {
			copyTaskList.add(new CopyTask(sourceConnection, targetConnectionList.get(i), sourceTableName,
					indexFirstColumnName, rowCountPerThread * i, rowCountPerThread, targetTableName));
		}
		copyTaskList.add(new CopyTask(sourceConnection, targetConnectionList.get(numThreads - 1), sourceTableName,
				indexFirstColumnName, rowCountPerThread * (numThreads - 1), rowCountPerThread + rowCountRemain,
				targetTableName));

		executorService = Executors.newFixedThreadPool(numThreads);

		List<Future<Long>> copyFutureList = null;
		try {
			copyFutureList = executorService.invokeAll(copyTaskList);

			Iterator<Future<Long>> copyFutureIter = copyFutureList.iterator();
			Iterator<XAResource> targetXAResourceIter = targetXAResourceList.iterator();
			Iterator<Xid> targetXidIter = targetXidList.iterator();

			while (copyFutureIter.hasNext() && targetXidIter.hasNext()) {
				Future<Long> copyFuture = copyFutureIter.next();
				XAResource targetXAResource = targetXAResourceIter.next();
				Xid targetXid = targetXidIter.next();
				
				long insertRowCount = copyFuture.get();
				if (insertRowCount < 0) {
					totalinsertRowCount = -1;
					break;
				} else {
					totalinsertRowCount += insertRowCount;
					
					try {
						targetXAResource.end(targetXid, XAResource.TMSUCCESS);
					} catch (XAException e) {
						e.printStackTrace();
					}
				}

				LOGGER.log(Level.INFO, "Finished processing " + totalinsertRowCount + " rows");
			}
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, "Interrupted while waiting for copy tasks to complete", e);
			return;
		} catch (ExecutionException e) {
			LOGGER.log(Level.SEVERE, "Execution error while waiting for copy task to complete", e);
			return;
		}

		executorService.shutdown();
		try {
			executorService.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, "Interrupted while waiting for executor services to terminate", e);
			return;
		}
	}

	private Xid createXid(int bids) {
		/*
		 * 같은 Global Transaction ID를 가지고, 다른 Branch Transaction ID를 갖는 Transaction ID 를
		 * 생성한다.
		 */

		byte[] gid = new byte[1];
		gid[0] = (byte) 9;

		byte[] bid = new byte[1];
		bid[0] = (byte) bids;

		/* GlobalTransactionId */
		byte[] gtrid = new byte[64];

		/* BracheQualifier */
		byte[] bqual = new byte[64];

		System.arraycopy(gid, 0, gtrid, 0, 1);
		System.arraycopy(bid, 0, bqual, 0, 1);

		Xid xid = new CUBRIDXid(0x1234, gtrid, bqual);

		return xid;
	}

	private String getIndexFirstColumnName(String sourceTableName) {
		String indexName = null;
		String indexFirstColumnName = null;

		boolean withOwnerName = false;
		String bindTableName = null;
		String bindOwnerName = null;

		int indexOfDot = sourceTableName.indexOf(".");
		if (indexOfDot > 0) {
			bindOwnerName = sourceTableName.substring(0, indexOfDot);
			bindTableName = sourceTableName.substring(indexOfDot + 1);
			withOwnerName = true;
		}

		String findIndexQuery = CUBRIDConnectionManager.getFindIndexQuery(withOwnerName);
		try (PreparedStatement sourceStatement = sourceConnection.prepareStatement(findIndexQuery)) {

			if (withOwnerName) {
				sourceStatement.setString(1, bindTableName);
				sourceStatement.setString(2, bindOwnerName);
			} else {
				sourceStatement.setString(1, sourceTableName);
			}

			try (ResultSet resultSet = sourceStatement.executeQuery()) {
				if (resultSet.next()) {
					indexName = resultSet.getString("index_name");
					indexFirstColumnName = resultSet.getString("key_attr_name");

					LOGGER.log(Level.INFO, String.format("Using the %s index.", indexName));
					return null;
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to execute query for source connection", e);
				return null;
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to create prepared statement for source connection", e);
			return null;
		}

		return indexFirstColumnName;
	}

	private boolean checkXAResource() {
		try {
			Iterator<XAResource> targetXAResourceIter = targetXAResourceList.iterator();
			Iterator<Xid> targetXidIter = targetXidList.iterator();

			while (targetXAResourceIter.hasNext() && targetXidIter.hasNext()) {
				XAResource targetXAResource = targetXAResourceIter.next();
				Xid targetXid = targetXidIter.next();
				
				if (targetXAResource.prepare(targetXid) != XAResource.XA_OK) {
					return false;
				}
			}
		} catch (XAException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}
	
	private void allXACommit() {
		try {
			Iterator<XAResource> targetXAResourceIter = targetXAResourceList.iterator();
			Iterator<Xid> targetXidIter = targetXidList.iterator();

			while (targetXAResourceIter.hasNext() && targetXidIter.hasNext()) {
				XAResource targetXAResource = targetXAResourceIter.next();
				Xid targetXid = targetXidIter.next();
				
				targetXAResource.commit(targetXid, false);
			}
		} catch (XAException e) {
			e.printStackTrace();
		}
		
		if (sourceConnection != null) {
			try {
				sourceConnection.commit();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to rollback source connection", e);
			}
		}
	}
	
	private void allXARollback() {
		try {
			Iterator<XAResource> targetXAResourceIter = targetXAResourceList.iterator();
			Iterator<Xid> targetXidIter = targetXidList.iterator();

			while (targetXAResourceIter.hasNext() && targetXidIter.hasNext()) {
				XAResource targetXAResource = targetXAResourceIter.next();
				Xid targetXid = targetXidIter.next();
				
				targetXAResource.rollback(targetXid);
			}
		} catch (XAException e) {
			e.printStackTrace();
		}
		
		if (sourceConnection != null) {
			try {
				sourceConnection.rollback();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to rollback source connection", e);
			}
		}
	}

	private void allCommit() {
		for (Connection targetConnection : targetConnectionList) {
			if (targetConnection != null) {
				try {
					targetConnection.commit();
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE, "Failed to commit target connection", e);
				}
			}
		}

		if (sourceConnection != null) {
			try {
				sourceConnection.commit();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to commit source connection", e);
			}
		}
	}

	private void allRollback() {
		for (Connection targetConnection : targetConnectionList) {
			if (targetConnection != null) {
				try {
					targetConnection.rollback();
				} catch (SQLException e) {
					LOGGER.log(Level.SEVERE, "Failed to rollback target connection", e);
				}
			}
		}

		if (sourceConnection != null) {
			try {
				sourceConnection.rollback();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to rollback source connection", e);
			}
		}
	}

	private void closeConnections() {
		if (targetConnectionList != null) {
			for (Connection targetConnection : targetConnectionList) {
				if (targetConnection != null) {
					try {
						targetConnection.close();
					} catch (SQLException e) {
						LOGGER.log(Level.SEVERE, "Failed to close target connection", e);
					}
				}
			}
		}

		if (targetXAConnectionList != null) {
			for (XAConnection targetXAConnection : targetXAConnectionList) {
				if (targetXAConnection != null) {
					try {
						targetXAConnection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}

		if (sourceConnection != null) {
			try {
				sourceConnection.close();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, "Failed to close source connection", e);
			}
		}
	}
}

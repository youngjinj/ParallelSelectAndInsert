package org.cubrid;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class ParallelSelectAndInsert {
	private static final Logger LOGGER = Logger.getLogger(ParallelSelectAndInsert.class.getName());

	public static final int DEFAULT_BATCH_COUNT = 1000;

	private ConnectionManager manager;
	private XidGenerator xidGenerator;

	private String sourceTableName;
	private String destinationTableName;
	private int numThreads;
	private int maxNumThreads;
	private int batchCount;

	private ProgressBarTask progressBar;
	
	private List<XAConnection> destinationXAConnectionList;
	private List<XAResource> destinationXAResourceList;
	private List<Xid> destinationXidList;
	private List<Connection> destinationConnectionList;
	private List<CopyTask> copyTaskList;
	private List<Future<Void>> copyFutureList;

	private ExecutorService executorService;

	public ParallelSelectAndInsert() {
		this.manager = new ConnectionManager();
		this.xidGenerator = new XidGenerator();
		this.maxNumThreads = Runtime.getRuntime().availableProcessors();
		this.batchCount = DEFAULT_BATCH_COUNT;
	}

	public void start(String paramSourceTableName, String paramDestinationTableName, int paramNumThreads, ProgressBarTask paramProgressBar) {
		LOGGER.log (Level.INFO, "Starting Parallel Select and Insert program");
		
		assert (manager != null);
		assert (xidGenerator != null);

		if (paramSourceTableName != null) {
			sourceTableName = paramSourceTableName;
		} else {
			LOGGER.log(Level.SEVERE, "Table name is null");
			return;
		}

		if (paramDestinationTableName != null) {
			destinationTableName = paramDestinationTableName;
		} else {
			destinationTableName = paramSourceTableName;
		}

		if (paramNumThreads > maxNumThreads) {
			numThreads = maxNumThreads;
		} else if (paramNumThreads > 0) {
			numThreads = paramNumThreads;
		} else {
			numThreads = 1;
		}
		
		if (paramProgressBar != null) {
			progressBar = paramProgressBar;
		} else {
			LOGGER.log(Level.SEVERE, "paramProgressBar is null");
			return;
		}

		assert (sourceTableName != null);
		assert (destinationTableName != null);
		assert (numThreads > 0);

		try (Connection sourceConnection = manager.getSourceConnection()) {
			assert (sourceConnection != null);

			sourceConnection.setAutoCommit(false);
			sourceConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

			long rowCount = manager.getTableRowCount(sourceConnection, sourceTableName);
			if (rowCount == 0) {
				LOGGER.log(Level.WARNING, "No data to copy");
				return;
			}
			rowCount = 100000;
			long rowCountPerThread = rowCount / numThreads;
			long rowCountRemain = rowCount % numThreads;
			
			progressBar.setTotalOfMain(rowCount);

			initXAResources(numThreads);
			assert (destinationXAConnectionList != null);
			assert (destinationXAResourceList != null);
			assert (destinationXidList != null);
			assert (destinationConnectionList != null);

			startXAResources();

			executorService = Executors.newFixedThreadPool(numThreads);

			String sourceIndexFirstColumnName = manager.getFirstColumnOfUsableIndex(sourceConnection, sourceTableName);

			copyTaskList = new ArrayList<CopyTask>(numThreads);
			for (int i = 0; i < numThreads; i++) {
				Connection destinationConnection = destinationConnectionList.get(i);
				assert (destinationConnectionList.get(i) != null);

				CopyTaskInfo copyTaskInfo = new CopyTaskInfo();
				copyTaskInfo.setThreadNum(i);
				copyTaskInfo.setSourceConnection(sourceConnection);
				copyTaskInfo.setSourceTableName(sourceTableName);

				if (sourceIndexFirstColumnName != null) {
					copyTaskInfo.setSourceIndexFirstColumnName(sourceIndexFirstColumnName);
				}

				if (i == (numThreads - 1)) {
					copyTaskInfo.setRowCount(rowCountPerThread + rowCountRemain);
					progressBar.setTotalPerThread(i, rowCountPerThread + rowCountRemain);
				} else {
					copyTaskInfo.setRowCount(rowCountPerThread);
					progressBar.setTotalPerThread(i, rowCountPerThread);
				}

				copyTaskInfo.setOffset(rowCountPerThread * i);
				copyTaskInfo.setDestinationConnection(destinationConnection);
				copyTaskInfo.setDestinationTableName(destinationTableName);
				copyTaskInfo.setBatchCount(batchCount);
				copyTaskInfo.setProgressBar(progressBar);

				copyTaskList.add(new CopyTask(copyTaskInfo));
			}

			try {
				copyFutureList = executorService.invokeAll(copyTaskList);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			assert (copyFutureList != null);

			for (int i = 0; i < numThreads; i++) {
				copyFutureList.get(i).get();
			}
			
			while (!progressBar.isFinish()) {
				;
			}
			
			Thread.sleep(1000);

			endXAResources();
			closeXAResources();
			
			executorService.shutdown();

			/* Because it is a select query, no commit is required. */
			sourceConnection.rollback();

			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (!executorService.isTerminated()) {
			executorService.shutdownNow();
		}

		try {
			rollbackXAResources();
			closeXAResources();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (XAException e) {
			e.printStackTrace();
		}

		return;
	}

	private void initXAResources(int numThreads)
			throws ClassNotFoundException, IllegalArgumentException, IOException, NullPointerException, SQLException {
		if (manager == null) {
			throw new NullPointerException("manager is null");
		}

		if (xidGenerator == null) {
			throw new NullPointerException("xidGenerator is null");
		}

		if (numThreads <= 0) {
			throw new IllegalArgumentException("The number of threads must be greater than 0");
		}

		destinationXAConnectionList = new ArrayList<XAConnection>(numThreads);
		destinationXAResourceList = new ArrayList<XAResource>(numThreads);
		destinationXidList = new ArrayList<Xid>(numThreads);
		destinationConnectionList = new ArrayList<Connection>(numThreads);

		for (int i = 0; i < numThreads; i++) {
			XAConnection xaConnection = manager.getDestinationXAConnection();
			XAResource xaResource = xaConnection.getXAResource();
			Xid xid = xidGenerator.generateXid();
			Connection connection = xaConnection.getConnection();
			connection.setAutoCommit(false);

			destinationXAConnectionList.add(xaConnection);
			destinationXAResourceList.add(xaResource);
			destinationXidList.add(xid);
			destinationConnectionList.add(connection);
		}
	}

	private void startXAResources() throws NullPointerException, XAException {
		if (destinationXAResourceList == null) {
			throw new NullPointerException("destinationXAResourceList is null");
		}

		if (destinationXidList == null) {
			throw new NullPointerException("destinationXidList is null");
		}

		Iterator<XAResource> xaResourceIter = destinationXAResourceList.iterator();
		Iterator<Xid> xidIter = destinationXidList.iterator();

		while (xaResourceIter.hasNext() && xidIter.hasNext()) {
			XAResource xaResource = xaResourceIter.next();
			Xid xid = xidIter.next();

			xaResource.start(xid, XAResource.TMNOFLAGS);
		}
	}

	private boolean prepareXAResources() throws NullPointerException, XAException {
		if (destinationXAResourceList == null) {
			throw new NullPointerException("destinationXAResourceList is null");
		}

		if (destinationXidList == null) {
			throw new NullPointerException("destinationXidList is null");
		}

		Iterator<XAResource> xaResourceIter = destinationXAResourceList.iterator();
		Iterator<Xid> xidIter = destinationXidList.iterator();

		while (xaResourceIter.hasNext() && xidIter.hasNext()) {
			XAResource xaResource = xaResourceIter.next();
			Xid xid = xidIter.next();

			if (xaResource.prepare(xid) != XAResource.XA_OK) {
				return false;
			}
		}

		return true;
	}

	private boolean commitXAResources() throws NullPointerException, XAException {
		if (destinationXAResourceList == null) {
			throw new NullPointerException("destinationXAResourceList is null");
		}

		if (destinationXidList == null) {
			throw new NullPointerException("destinationXidList is null");
		}

		Iterator<XAResource> xaResourceIter = destinationXAResourceList.iterator();
		Iterator<Xid> xidIter = destinationXidList.iterator();

		while (xaResourceIter.hasNext() && xidIter.hasNext()) {
			XAResource xaResource = xaResourceIter.next();
			Xid xid = xidIter.next();

			xaResource.commit(xid, false);
		}

		return true;
	}

	private boolean rollbackXAResources() throws NullPointerException, XAException {
		if (destinationXAResourceList == null) {
			throw new NullPointerException("destinationXAResourceList is null");
		}

		if (destinationXidList == null) {
			throw new NullPointerException("destinationXidList is null");
		}

		Iterator<XAResource> xaResourceIter = destinationXAResourceList.iterator();
		Iterator<Xid> xidIter = destinationXidList.iterator();

		while (xaResourceIter.hasNext() && xidIter.hasNext()) {
			XAResource xaResource = xaResourceIter.next();
			Xid xid = xidIter.next();

			xaResource.rollback(xid);
		}

		return true;
	}

	private void endXAResources() throws NullPointerException, XAException {
		if (destinationXAResourceList == null) {
			throw new NullPointerException("destinationXAResourceList is null");
		}

		if (destinationXidList == null) {
			throw new NullPointerException("destinationXidList is null");
		}

		Iterator<XAResource> xaResourceIter = destinationXAResourceList.iterator();
		Iterator<Xid> xidIter = destinationXidList.iterator();

		while (xaResourceIter.hasNext() && xidIter.hasNext()) {
			XAResource xaResource = xaResourceIter.next();
			Xid Xid = xidIter.next();

			xaResource.end(Xid, XAResource.TMSUCCESS);
		}

		boolean isSuccess = prepareXAResources();
		if (isSuccess) {
			commitXAResources();
		} else {
			rollbackXAResources();
		}
	}

	private void closeXAResources() throws SQLException {
		if (destinationConnectionList != null) {
			for (Connection connection : destinationConnectionList) {
				if (connection != null) {
					connection.close();
				}
			}
		}

		if (destinationXAConnectionList != null) {
			for (XAConnection xaConnection : destinationXAConnectionList) {
				if (xaConnection != null) {
					xaConnection.close();
				}
			}
		}
	}
}

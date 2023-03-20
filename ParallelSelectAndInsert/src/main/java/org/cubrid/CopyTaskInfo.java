package org.cubrid;

import java.sql.Connection;

public class CopyTaskInfo {
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
	
	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}
	
	public Connection getSourceConnection() {
		return sourceConnection;
	}

	public void setSourceConnection(Connection sourceConnection) {
		this.sourceConnection = sourceConnection;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public String getSourceIndexFirstColumnName() {
		return sourceIndexFirstColumnName;
	}

	public void setSourceIndexFirstColumnName(String sourceIndexFirstColumnName) {
		this.sourceIndexFirstColumnName = sourceIndexFirstColumnName;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getRowCount() {
		return rowCount;
	}

	public void setRowCount(long rowCount) {
		this.rowCount = rowCount;
	}

	public Connection getDestinationConnection() {
		return destinationConnection;
	}

	public void setDestinationConnection(Connection destinationConnection) {
		this.destinationConnection = destinationConnection;
	}

	public String getDestinationTableName() {
		return destinationTableName;
	}

	public void setDestinationTableName(String destinationTableName) {
		this.destinationTableName = destinationTableName;
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}
	
	public ProgressBarTask getProgressBar() {
		return progressBar;
	}

	public void setProgressBar(ProgressBarTask progressBar) {
		this.progressBar = progressBar;
	}
}

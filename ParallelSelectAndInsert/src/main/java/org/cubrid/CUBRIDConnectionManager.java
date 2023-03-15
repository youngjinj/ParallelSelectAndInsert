package org.cubrid;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.XAConnection;

import cubrid.jdbc.driver.CUBRIDXADataSource;

public class CUBRIDConnectionManager {
	private static final Logger LOGGER = Logger.getLogger(CUBRIDConnectionManager.class.getName());

	public static Connection getSourceConnection() {
		Properties properties = new Properties();
		Connection connection = null;

		try (Reader reader = new FileReader("databases.properties")) {
			properties.load(reader);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Failed to read properties file", e);
			return null;
		}

		String url = properties.getProperty("SourceUrl");
		String user = properties.getProperty("SourceUser");
		String password = properties.getProperty("SourcePassword");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Failed to load driver class", e);
			return null;
		}

		try {
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to connect to source database", e);
			return null;
		}

		return connection;
	}

	public static Connection getTargetConnection() {
		Properties properties = new Properties();
		Connection connection = null;

		try (Reader reader = new FileReader("databases.properties")) {
			properties.load(reader);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Failed to read properties file", e);
			return null;
		}

		String url = properties.getProperty("TargetUrl");
		String user = properties.getProperty("TargetUser");
		String password = properties.getProperty("TargetPassword");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Failed to load driver class", e);
			return null;
		}

		try {
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to connect to target database", e);
			return null;
		}

		return connection;
	}

	public static XAConnection getTargetXAConnection() {
		Properties properties = new Properties();
		CUBRIDXADataSource xaDataSource = null;
		XAConnection xaConnection = null;

		try (Reader reader = new FileReader("databases.properties")) {
			properties.load(reader);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Failed to read properties file", e);
			return null;
		}

		String serverName = properties.getProperty("TargetServerName");
		int portNumber = Integer.parseInt(properties.getProperty("TargetPortNumber"));
		String databaseName = properties.getProperty("TargetDatabaseName");
		String user = properties.getProperty("TargetUser");
		String password = properties.getProperty("TargetPassword");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Failed to load driver class", e);
			return null;
		}

		try {
			xaDataSource = new CUBRIDXADataSource();
			xaDataSource.setServerName(serverName);
			xaDataSource.setPortNumber(portNumber);
			xaDataSource.setDatabaseName(databaseName);
			xaDataSource.setUser(user);
			xaDataSource.setPassword(password);
			
			xaConnection = xaDataSource.getXAConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return xaConnection;
	}
	
	public static String getFindIndexQuery(boolean withOwnerName) {
		StringBuilder query = new StringBuilder();
		query.append("select a.index_name, a.is_primary_key, b.key_attr_name").append(" ");
		query.append("from db_index as a, db_index_key as b, db_attribute as c").append(" ");
		query.append("where").append(" ");
		query.append("a.index_name = b.index_name").append(" ");
		query.append("and a.class_name = b.class_name").append(" ");
		query.append("and a.owner_name = b.owner_name").append(" ");
		query.append("and b.class_name = c.class_name").append(" ");
		query.append("and b.owner_name = c.owner_name").append(" ");
		query.append("and b.key_attr_name = c.attr_name").append(" ");
		query.append("and a.class_name = ?").append(" ");
		if (withOwnerName) {
			query.append("and a.owner_name = upper(?)").append(" ");
		} else {
			query.append("and a.owner_name = current_user").append(" ");
		}
		query.append("and a.filter_expression is null").append(" ");
		query.append("and a.have_function = 'NO'").append(" ");
		query.append("and a.status != 'INVISIBLE INDEX'").append(" ");
		query.append("and b.key_order = 0").append(" ");
		query.append("and c.is_nullable = 'NO'").append(" ");
		query.append("order by a.is_primary_key desc").append(" ");
		query.append("limit 1").append(" ");
		return query.toString();
	}
	
	public static String getCountQuery(String tableName) {
		StringBuilder query = new StringBuilder();
		query.append("select count(*) from ").append(tableName);
		return query.toString();
	}
	
	public static String getSelectQuery(String tableName) {
		StringBuilder query = new StringBuilder();
		query.append("select * from ").append(tableName).append(" limit ?, ?");
		return query.toString();
	}

	public static String getSelectQueryUsingIndex(String tableName, String columnName) {
		StringBuilder query = new StringBuilder();
		query.append("select /*+ USE_IDX */ * from ").append(tableName).append(" where ").append(columnName)
				.append(" = ").append(columnName).append(" order by ").append(columnName).append(" limit ?, ?");
		return query.toString();
	}

	public static String getInsertQuery(String tableName, int columnCount) {
		StringBuilder query = new StringBuilder();
		query.append("insert into ").append(tableName).append(" values (");
		if (columnCount > 0) {
			query.append("?");
		}
		for (int i = 1; i < columnCount; i++) {

			query.append(", ").append("?");
		}
		query.append(")");
		return query.toString();
	}
}

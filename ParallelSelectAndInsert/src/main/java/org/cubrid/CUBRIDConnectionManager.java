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

public class CUBRIDConnectionManager {
	private static final Logger LOGGER = Logger.getLogger(CUBRIDConnectionManager.class.getName());

	public static Connection getSourceConnection() {
		Properties properties = new Properties();
		Connection connection = null;

		try (Reader reader = new FileReader("databases.properties")) {
			properties.load(reader);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Failed to read properties file", e);
		}

		String url = properties.getProperty("SourceUrl");
		String user = properties.getProperty("SourceUser");
		String password = properties.getProperty("SourcePassword");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Failed to load driver class", e);
		}

		try {
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to connect to source database", e);
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
		}

		String url = properties.getProperty("TargetUrl");
		String user = properties.getProperty("TargetUser");
		String password = properties.getProperty("TargetPassword");

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Failed to load driver class", e);
		}

		try {
			connection = DriverManager.getConnection(url, user, password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, "Failed to connect to target database", e);
		}

		return connection;
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
				.append(" = ").append(columnName).append(" order by c1 limit ?, ?");
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

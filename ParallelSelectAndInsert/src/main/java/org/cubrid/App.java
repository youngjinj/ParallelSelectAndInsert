package org.cubrid;

import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
	private static final Logger LOGGER = Logger.getLogger(App.class.getName());

	public static void main(String[] args) {
		int numThreads = Runtime.getRuntime().availableProcessors();
		numThreads = 3;
		
		LOGGER.setLevel(Level.INFO);

		long startTime = System.currentTimeMillis();

		String sourceTableName = "t1"; // "road_name_address";
		String targetTableName = sourceTableName;

		ParallelSelectAndInsert parallelSelectAndInsert = new ParallelSelectAndInsert(numThreads);
		parallelSelectAndInsert.start(sourceTableName, targetTableName);

		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		System.out.println("ElapsedTime : " + elapsedTime + " ms");
	}
}

package org.cubrid;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App {
	private static final Logger LOGGER = Logger.getLogger(App.class.getName());

	private static final String DEFAULT_TABLE_NAME = "road_name_address";

	public static void main(String[] args) {
		Option optHelp = Option.builder("h").longOpt("help").desc("Displays the help message for the program")
				.hasArg(false).build();
		Option optSourceTableName = Option.builder("s").longOpt("source-table-name")
				.desc("Specifies the name of the source table to fetch records from").hasArg(true).build();
		Option optDestinationTableName = Option.builder("d").longOpt("dest-table-name")
				.desc("Specifies the name of the destination table to insert records to").hasArg(true).build();
		Option optNumThreads = Option.builder("t").longOpt("thread-count")
				.desc("Specifies the number of threads to use for concurrent processing").hasArg(true).build();

		Options options = new Options();
		options.addOption(optHelp);
		options.addOption(optSourceTableName);
		options.addOption(optDestinationTableName);
		options.addOption(optNumThreads);

		HelpFormatter formatter = new HelpFormatter();

		String sourceTableName = null;
		String destinationTableName = null;
		int numThreads = -1;

		try {
			CommandLineParser parser = new DefaultParser();
			CommandLine command = parser.parse(options, args);

			if (command.hasOption("h")) {
				formatter.printHelp("ParallelSelectAndInsert", options);
				return;
			} else {
				optSourceTableName.setRequired(true);
			}

			options.addOption(optSourceTableName);
			command = parser.parse(options, args);

			if (command.hasOption("s")) {
				sourceTableName = command.getOptionValue("s");
			}

			if (command.hasOption("d")) {
				destinationTableName = command.getOptionValue("d");
			} else {
				destinationTableName = sourceTableName;
			}

			if (command.hasOption("t")) {
				try {
					numThreads = Integer.parseInt(command.getOptionValue("t"));
					
					/**
					 * Retrieves the number of available logical CPU cores, 
					 * limiting the count to a maximum of half the total if hyper-threading is enabled.
					 */
					int availableProcessors = Runtime.getRuntime().availableProcessors() / 2;
					if (availableProcessors == 0) {
						availableProcessors = 1;
					}

					if (numThreads > availableProcessors) {
						numThreads = availableProcessors;
					}
				} catch (NumberFormatException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
					return;
				}
			} else {
				numThreads = 1;
			}
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			formatter.printHelp("ParallelSelectAndInsert", options);
			return;
		}

		/*-
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.contains("win")) {
			AnsiConsole.systemInstall();
		}
		*/

		Instant start = Instant.now();

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		ProgressBarTask progressBar = new ProgressBarTask(numThreads);
		Future<Void> future = executorService.submit(progressBar);
		assert (future != null);

		ParallelSelectAndInsert parallelSelectAndInsert = new ParallelSelectAndInsert();
		parallelSelectAndInsert.start(sourceTableName, destinationTableName, numThreads, progressBar);

		executorService.shutdownNow();

		Instant end = Instant.now();
		Duration duration = Duration.between(start, end);

		long seconds = duration.getSeconds();
		long absSeconds = Math.abs(seconds);
		String formattedTime = String.format("%02d:%02d:%02d", absSeconds / 3600, (absSeconds % 3600) / 60,
				absSeconds % 60);

		if (seconds < 0) {
			formattedTime = "-" + formattedTime;
		}
		System.out.println("");
		System.out.println("Elapsed time: " + formattedTime);
		System.out.println("");

		/*-
		if (osName.contains("win")) {
			AnsiConsole.systemUninstall();
		}
		*/
	}
}

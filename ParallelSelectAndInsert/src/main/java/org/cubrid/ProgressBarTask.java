package org.cubrid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class ProgressBarTask implements Callable<Void> {
	private int numThreads;
	private List<String> threadNameList;
	private List<Long> progressPerThreadList;
	private List<Long> totalPerThreadList;

	public ProgressBarTask(int numThreads) {
		this.numThreads = numThreads;
		this.threadNameList = new ArrayList<String>();
		this.progressPerThreadList = new ArrayList<Long>();
		this.totalPerThreadList = new ArrayList<Long>();

		for (int i = 0; i < numThreads; i++) {
			this.threadNameList.add("Thread-" + (i + 1));
			this.progressPerThreadList.add(0L);
			this.totalPerThreadList.add(0L);
		}

		this.threadNameList.add("Main");
		this.progressPerThreadList.add(0L);
		this.totalPerThreadList.add(0L);
	}

	@Override
	public Void call() {
		Terminal terminal = null;
		try {
			terminal = TerminalBuilder.terminal();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (!Thread.currentThread().isInterrupted()) {
			updateProgressBar(terminal);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				return null;
			}
		}

		return null;
	}

	private void updateProgressBar(Terminal terminal) {
		terminal.writer().println();

		for (int i = 0; i < numThreads + 1; i++) {
			String threadName = threadNameList.get(i);
			long progressPerThread = progressPerThreadList.get(i);
			long totalPerThread = totalPerThreadList.get(i);

			/* ANSI escape code */
			terminal.writer()
					.println(String.format("[%10s] %s %10s/%-10s ( -%s )", threadName,
							getProgressBar(progressPerThread, totalPerThread), progressPerThread, totalPerThread,
							(totalPerThread - progressPerThread)));
		}

		terminal.writer().println();
		terminal.flush();
	}

	/*-
	<!-- https://mvnrepository.com/artifact/org.fusesource.jansi/jansi -->
	<dependency>
		<groupId>org.fusesource.jansi</groupId>
		<artifactId>jansi</artifactId>
		<version>2.4.0</version>
	</dependency>
	*/

	/*-
	private void updateProgressBarWithANSIEscape(Terminal terminal) {
		int heigth = terminal.getHeight();
		int outputX = heigth - (numThreads + 1);
	
		int oldX = terminal.getCursorPosition(null).getY();
	
		for (int i = 0; i < numThreads + 1; i++) {
			String threadName = threadNameList.get(i);
			long progressPerThread = progressPerThreadList.get(i);
			long totalPerThread = totalPerThreadList.get(i);
	
			// ANSI escape code
			terminal.writer().print("\033[" + (outputX + i) + ";" + 0 + "H");
			terminal.writer().print("\033[" + 2 + "K");
			terminal.writer()
					.print(String.format("[%10s] %s %10s/%-10s ( -%s )", threadName,
							getProgressBar(progressPerThread, totalPerThread), progressPerThread, totalPerThread,
							(totalPerThread - progressPerThread)));
		}
	
		terminal.writer().print("\033[" + (oldX + 1) + ";" + 0 + "H"); // move
		terminal.flush();
	}
	*/

	public boolean isFinish() {
		long progress = progressPerThreadList.get(numThreads);
		long total = totalPerThreadList.get(numThreads);

		if (progress == total) {
			return true;
		}

		return false;
	}

	private String getProgressBar(long progress, long total) {
		StringBuilder progressBar = new StringBuilder();

		int progressPercentage = (int) (((double) progress / (double) total) * 100);
		int numberOfBars = progressPercentage / 5;

		progressBar.append("[");

		for (int i = 0; i < numberOfBars; i++) {
			progressBar.append("=");
		}

		for (int i = 0; i < 20 - numberOfBars; i++) {
			progressBar.append(" ");
		}

		progressBar.append("]").append(" ").append(String.format("%3s", progressPercentage)).append("%");

		return progressBar.toString();
	}

	public synchronized void setProgressPerThread(int threadNum, long progressPerThread) {
		progressPerThreadList.set(threadNum, progressPerThread);
	}

	public synchronized void setTotalPerThread(int threadNum, long totalPerThread) {
		totalPerThreadList.set(threadNum, totalPerThread);
	}

	public synchronized void addProgressOfMain(long progress) {
		long oldProgress = this.progressPerThreadList.get(numThreads);
		progressPerThreadList.set(numThreads, oldProgress + progress);
	}

	public synchronized void setTotalOfMain(long total) {
		totalPerThreadList.set(numThreads, total);
	}
}

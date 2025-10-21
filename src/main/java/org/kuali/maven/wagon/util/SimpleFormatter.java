/**
 *
 */
package org.kuali.maven.wagon.util;

import java.util.concurrent.TimeUnit;

/**
 * Minimal formatter for human-readable time, size and throughput.
 */
public class SimpleFormatter {

	public String getTime(long millis) {
		if (millis < 1000) {
			return millis + " ms";
		}
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(hours);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis));
		StringBuilder sb = new StringBuilder();
		if (hours > 0) {
			sb.append(hours).append("h ");
		}
		if (minutes > 0 || hours > 0) {
			sb.append(minutes).append("m ");
		}
		sb.append(seconds).append("s");
		return sb.toString().trim();
	}

	public String getSize(long bytes) {
		final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
		double size = bytes;
		int unitIndex = 0;
		while (size >= 1024 && unitIndex < units.length - 1) {
			size /= 1024.0;
			unitIndex++;
		}
		return String.format("%.2f %s", size, units[unitIndex]);
	}

	public String getRate(long millis, long bytes) {
		if (millis <= 0) {
			return "-";
		}
		double seconds = millis / 1000.0;
		double bytesPerSecond = bytes / seconds;
		return getSize((long) bytesPerSecond) + "/s";
	}
}



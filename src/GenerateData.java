

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Random;

public class GenerateData {

	/** Prints usage and exits.  */
	static void usage() {
		System.err.println("Usage: generate metric num_days pph [csv]");
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3)
			usage();
		
		String metricName = args[0];
		int numMetrics = 1;
		int numTagK = 1;
		int numTagV = 1;
		int startYear = 2010;
		int totalYears = 1;
		int days = Integer.parseInt(args[1]);
		int pph = Integer.parseInt(args[2]);
		int range = 101;
		int gap = 50;
		boolean csv = args.length > 3 && "csv".equals(args[3]);
		
		if (pph <= 0) {
			System.err.println("pph must be a positive number");
			System.exit(-1);
		}

		System.out.printf("Generating %d days for metric %s with %d points per hour\n", days, metricName, pph);

		generateYearlyFiles(metricName, numMetrics, numTagK, numTagV, startYear, totalYears, days, pph, range, gap, new Random(), csv);
	}

	public static void generateYearlyFiles(final String metricName, final int numMetrics, final int numTagK, final int numTagV, final int startYear, final int totalYears, final int days, final int pph, final int range, final int gap, final Random rand, final boolean csv) throws IOException {
		//TODO add timezone (--tz) param to argp
		Calendar cal = Calendar.getInstance(); // use local timezone
		cal.set(startYear, 0, 1, 0, 0, 0);

		String extension = csv ? ".csv" : ".tsd";

		long value = rand.nextInt(range) - gap;
		long count = 0;
		
		int[] tagValues = new int[numTagK]; 

		long startTime = System.currentTimeMillis();

		for (int year = startYear; year < startYear + totalYears; year++) {

			File metricFile = new File(metricName + extension);
			OutputStream os = createOutputStream(metricFile);

			long time = (pph > 3600) ? cal.getTimeInMillis() : cal.getTimeInMillis() / 1000;
			int time_inc = (pph > 3600) ? 3600000 / pph : 3600 / pph;
			
				for (int day = 0; day < days; day++) {
					System.out.printf("Day %d / %d\n", day+1, days);
					for (int hour = 0; hour < 24; hour++) {
						for (int i = 0; i < pph; i++) {
							
							for (int v = 0; v < numTagK; v++) {
								tagValues[v] = rand.nextInt(numTagV);
							}
							
							final String mname = metricName + ((numMetrics > 1) ? "." + rand.nextInt(numMetrics) : "");
							
							if (csv)
								writeRecordCSV(os, mname, time, value, tagValues);
							else
								writeRecord(os, mname, time, value, tagValues);
							
							// Alter the value by a range of +/- RANDOM_GAP
							value += rand.nextInt(range) - gap;
							
							time+= time_inc;
							
							count++;
						}
					}
				}

			os.flush();
			os.close();

			cal.add(Calendar.YEAR, 1);
		}
		long totalTime = System.currentTimeMillis() - startTime;
		//TODO display total number of data points
		System.out.printf("Total time to create %d data points: %dms\n", count, totalTime);
	}

	private static OutputStream createOutputStream(File path) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);
		return new BufferedOutputStream(fos);
	}

	private static void writeRecord(OutputStream os, String metricName, long time, long value, int[] tagValues) throws IOException {
		StringBuffer record = new StringBuffer();
		record.append(metricName)
		.append(" ")
		.append(time)
		.append(" ")
		.append(value);
		
		for (int v = 0; v < tagValues.length; v++) {
			record.append(" ")
			.append("tag")
			.append(v)
			.append("=")
			.append("value")
			.append(tagValues[v]);
		}
		
		record.append("\n");
		
		os.write(record.toString().getBytes());
	}

	private static void writeRecordCSV(final OutputStream os, final String metricName, final long time, final long value, final int[] tagValues) throws IOException {
		StringBuffer record = new StringBuffer();
		record.append('"').append(metricName).append('"')
		.append(", ")
		.append(time)
		.append(", ")
		.append(value);
		
		for (int v = 0; v < tagValues.length; v++) {
			record.append(", \"")
			.append("tag")
			.append(v)
			.append("=")
			.append("value")
			.append(tagValues[v])
			.append('"');
		}
		
		record.append("\n");
		
		os.write(record.toString().getBytes());
	}
}

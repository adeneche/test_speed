import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Simple tool to try various file reading strategies and compute average speed
 * 
 */
public class SpeedTest {
	public static final char SEPARATOR_CHAR = '\t';

	private static void usage() {
		System.err.println("usage: SpeedTest path N bufSize [more bufSize]");
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		// Input:
		//		path: file to load
		//		N: num trials per strategy
		//		bufSize: buffer size
		if (args.length < 3)
			usage();

		final String path = args[0];
		final int N = Integer.parseInt(args[1]);
		final int[] bufSizes = new int[args.length - 2];
		for (int i = 2; i < args.length; i++) {
			bufSizes[i-2] = Integer.parseInt(args[i]);
		}

		final int numCores = Runtime.getRuntime().availableProcessors();

		System.out.println("v 1.5");
		System.out.println("Number of Available cores: "+numCores);
		System.out.println("Counting lines...");
		final int numLines = countNumLines(path);
		System.out.printf("File has a size of %dB and contains %d lines \n", new File(path).length(), numLines);

		final AbstractStrategy[] strategies = {
				//new BufferLoad(N),
				//				new CharChannel(N, false, numLines),
				//				new CharChannel(N, true, numLines),
				//				new AntlrLoad(N, false),
				//				new AntlrLoad(N, true),
				//				new SimpleBytes(N),
				//new SimpleByteLineParser(N, numLines),
				new BufferCharLoader(N, 1),
				new BufferCharLoader(N, 2),
				//new SimpleByteWordsParser(N),
				//				new AntlrLoad(N, true),
				//				new ByteBufferLoad(N, false, numLines),
				//				new ByteBufferLoad(N, true, numLines),
				//				new ByteParser(N, true, numLines)
		};

		for (AbstractStrategy strategy : strategies) {
			Runtime.getRuntime().gc();

			System.out.println("Loading File using "+strategy);

			double min_mean = Double.MAX_VALUE;
			String min_description = "";
			int min_bufSize = 0;

			if (strategy.useBuffer) {
				for (int bufSize : bufSizes) {
					final double mean = strategy.load(path, bufSize);
					if (mean < min_mean) {
						min_mean = mean;
						min_bufSize = bufSize;
						min_description = strategy.describeBest();
					}
				}
			} else {
				min_mean = strategy.load(path, -1);
				min_bufSize = -1;
				min_description = strategy.describeBest();
			}

			displayAvgSpeed("Best Speed for bufSize = "+min_bufSize + min_description, min_mean, numLines);
		}
	}

	/**
	 * parse the file to compute numDP (number of lines)
	 */
	private static int countNumLines(final String path) throws Exception {
		final InputStream is = new FileInputStream(path);
		final BufferedReader in = new BufferedReader(new InputStreamReader(is));

		int numLines = 0;
		while (in.readLine() != null) {
			numLines++;
		}

		in.close();

		return numLines;
	}

	private static void displayAvgSpeed(final String msg, final double time_delta, final int points) {
		System.out.println(String.format(msg + " : %d data points in %.3fs (%,.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	static abstract class AbstractStrategy {

		final int N;
		final boolean useBuffer;

		AbstractStrategy(final int N, final boolean useBuffer) {
			this.N = N;
			this.useBuffer = useBuffer;
		}

		abstract double load(final String path, final int bufSize) throws Exception;

		public String describeBest() {
			return "";
		}

	}

	static class BufferCharLoader extends AbstractStrategy {
		final int threads;
		
		BufferCharLoader(int N, final int threads) {
			super(N, true);
			this.threads = threads;
		}

		@Override
		double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.currentTimeMillis();

				if (threads > 1)
					readInChunksMT(path, bufSize);
				else
					readInChunks(path, bufSize);
					
				results[i] = (System.currentTimeMillis() - start_time) / 1000.0;
			}

			return Utils.mean(results);
		}

		void readInChunks(final String path, final int bufSize) throws IOException {
			final File f = new File(path);
			final int size = (int)f.length();

			final FileInputStream is = new FileInputStream(path);

			final byte[] data = new byte[bufSize];
			int readCount = 0;
			int totalLines = 0;

			try {
				while (readCount < size) {
					int n = is.read(data);
					final List<byte[]> lines = ExtractLines(data, n);
					readCount += data.length;
					totalLines += lines.size(); 
				}
			}
			finally {
				is.close();
			}

			System.out.println("totalLines: "+totalLines);

		}

		void readInChunksMT(final String path, final int bufSize) throws IOException, InterruptedException, ExecutionException {
//			final long start_time = System.currentTimeMillis();
			
			final File f = new File(path);
			final int size = (int)f.length();
			final int numChunks = size / bufSize;

			final FileInputStream is = new FileInputStream(path);

			final byte[] data = new byte[bufSize];
			int readCount = 0;
			int totalLines = 0;
			
//			System.out.printf("read %d chunks\n", numChunks);

			final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
			List<Future<ParsedResult>> results = new Vector<>(numChunks);
			
//			final Runtime runtime = Runtime.getRuntime();
//			runtime.gc();
//			final long usedmem = runtime.totalMemory() -  runtime.freeMemory();

			try {
				int id = 0;
				int off = 0;
				while (readCount < size) {
					// make sure each chunk contains complete lines
					int n = is.read(data, off, bufSize - off);
					int total = off + n;
					
					// find the last '\n' in data
					int len = total;
					while (data[len-1] != '\n') len--;
					
					// only copy a complete block
					final byte[] chunk = Arrays.copyOf(data, len);
					
					// copy the incomplete line to the start of data
					if (off > 0) {
						for (int i = len; i < total; i++) {
							data[i - len] = data[i];
						}
					}
					off = total - len;
					
					final Future<ParsedResult> contentFuture = pool.submit(new ChunkParser(id++, chunk));
					results.add(contentFuture);
					
					readCount += n;
				}

				//TODO because we first read all chunks into memory before starting the next step (processing and sending to tsdb)
				// we will be storing all file bytes into memory in separate chunks of bytes
				// (fix 1) should help reduce the total memory used by the processed data points
//				System.out.printf("submitted all tasks (%d/%d) in %.2fs\n", pool.getCompletedTaskCount(), numChunks, (System.currentTimeMillis() - start_time)/1000.0);
				
				for (Future<ParsedResult> contentFuture : results) {
					final ParsedResult res = contentFuture.get();
					totalLines += res.size();
					//System.out.println("finished chunk N° "+res.id);
				}
				
//				runtime.gc();
//				System.out.printf("Use memory: %dB", (runtime.totalMemory() - runtime.freeMemory()) - usedmem);
			}
			finally {
				pool.shutdownNow();
				is.close();
			}

			System.out.printf("threads: %d, num chunks: %d, totalLines: %d\n", pool.getLargestPoolSize(), numChunks, totalLines);
		}

		@Override
		public String toString() {
			return "Buffer Char Loader" + (threads > 0 ? " with "+threads+" threads":"");
		}
	}
	
	private static List<byte[]> ExtractLines(final byte[] data, final int length) {
		final List<byte[]> lines = new ArrayList<>();

		int startIdx = 0;

		for (int idx = 0; idx < length; idx++) {
			final byte b = data[idx];
			//TODO don't we need to take care of '\r\n' like BufferedReader does ?
			if (b == '\r' || b == '\n') { // new line
				if (startIdx < idx) {
					final byte[] _line = Arrays.copyOfRange(data, startIdx, idx);
//					Utils.splitString(data, startIdx, idx-startIdx, (byte) SEPARATOR_CHAR);
					lines.add(_line);
				}
				startIdx = idx+1;
			}
		}
		return lines;
	}

	static class ParsedResult {
		//TODO the problem here is that we will need more space to store the data from the file
		// because each line will be in a separate byte array
		// one solution would be to use a ByteBuffer and only store start,length of each line into an int list
		// (fix 1) should help fix this because we'll be storing one single byte[] that contains all TimeValue
		// of the current chunk
		final int id;
		final byte[] data;
		final int len;
//		final List<Integer> start; // index of first byte of each line
//		final List<Integer> length; // length in bytes of each line, '\n' excluded
		
		public int size() {
//			return start.size();
			return len;
		}
		
		public ParsedResult(final int id, final byte[] data, final int len/*, final List<Integer> start, final List<Integer> length*/) {
			this.id = id;
			this.data = data;
			this.len = len;
//			this.start = start;
//			this.length = length;
		}
	}
	
	static class ChunkParser implements Callable<ParsedResult> {
		byte[] data;
		final int id;
		
		ChunkParser(final int id, final byte[] data) {
			this.data = data;
			this.id = id;
		}
		
		@Override
		public ParsedResult call() throws Exception {
//			final List<Integer> start = new ArrayList<>(1000);
//			final List<Integer> length = new ArrayList<>(1000);

			int startIdx = 0;
			int numlines = 0;
			
			for (int idx = 0; idx < data.length; idx++) {
				final byte b = data[idx];
				//TODO don't we need to take care of '\r\n' like BufferedReader does ?
				if (b == '\r' || b == '\n') { // new line
					if (startIdx < idx) {
//						Utils.splitString(data, startIdx, idx-startIdx, (byte) SEPARATOR_CHAR);
//						start.add(startIdx);
//						length.add(idx-startIdx);
						
						numlines++;
					}
					startIdx = idx+1;
				}
			}

		//	data = null;
			
			return new ParsedResult(id, data, numlines/*, start, length*/);
		}
	}

	static class SimpleByteLineParser extends AbstractStrategy {
		final int numLines;

		SimpleByteLineParser(int N, int numLines) {
			super(N, false);
			this.numLines = numLines;
		}

		@Override
		double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				final byte[] bytes = readAllBytes(path);
				final long total = parseBytes(bytes);

				//				System.out.println("total words parsed: "+total);

				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}

			return Utils.mean(results);
		}

		private byte[] readAllBytes(final String fileName) throws Exception {
			File f = new File(fileName);
			int size = (int)f.length();

			FileInputStream fis = new FileInputStream(fileName);

			byte[] data = null;
			try {
				data = new byte[size];
				int n = fis.read(data);
				if (n < data.length) {
					data = Arrays.copyOf(data, n);
				}
			}
			finally {
				fis.close();
			}

			return data;
		}

		private long parseBytes(final byte[] bytes) {
			final byte _r = '\r';
			final byte _n = '\n';

			int startIdx = 0;
			long total = 0;

			for (int idx = 0; idx < bytes.length; idx++) {
				final byte b = bytes[idx];
				if (b == _r || b == _n) { // new line
					if (startIdx < idx) {
						//						final byte[] _line = Arrays.copyOfRange(bytes, startIdx, idx);
						final String[] _words = Utils.splitString(bytes, startIdx, idx-startIdx, (byte) SEPARATOR_CHAR);

						//						for (byte[] word : _words) {
						//							if (word.length > 0) total++;
						//						}
						total += _words.length;
					}
					startIdx = idx+1;
				}
			}

			return total;
		}

		@Override
		public String toString() {
			return "Simple Bytes Lines Parser";
		}

	}

	static class SimpleByteWordsParser extends AbstractStrategy {

		SimpleByteWordsParser(int N) {
			super(N, false);
		}

		@Override
		double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				final byte[] bytes = readAllBytes(path);
				final double total = Utils.parseBytes(bytes);

				//				System.out.println("total words parsed: "+total);

				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}

			return Utils.mean(results);
		}

		private byte[] readAllBytes(final String fileName) throws Exception {
			File f = new File(fileName);
			int size = (int)f.length();

			FileInputStream fis = new FileInputStream(fileName);

			byte[] data = null;
			try {
				data = new byte[size];
				int n = fis.read(data);
				if (n < data.length) {
					data = Arrays.copyOf(data, n);
				}
			}
			finally {
				fis.close();
			}

			return data;
		}

		@Override
		public String toString() {
			return "Simple Bytes Words Parser";
		}

	}

	static class SimpleBytes extends AbstractStrategy {
		SimpleBytes(final int N) {
			super(N, false);
		}

		@Override
		public double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				readAllBytes(path);
				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}

			return Utils.mean(results);
		}

		private void readAllBytes(final String fileName) throws Exception {
			File f = new File(fileName);
			int size = (int)f.length();

			FileInputStream fis = new FileInputStream(fileName);

			byte[] data = null;
			try {
				data = new byte[size];
				/*int n =*/ fis.read(data);
				//				if (n < data.length) {
				//					data = Arrays.copyOf(data, n);
				//				}
			}
			finally {
				fis.close();
			}
		}

		@Override
		public String toString() {
			return "Simple Bytes";
		}


	}

	static class BufferLoad extends AbstractStrategy {

		BufferLoad(final int N) {
			super(N, false);
		}

		@Override
		public double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				final InputStream is = new FileInputStream(path);
				final BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), 102400);

				String line;
				int totalWords = 0;
				while ((line = in.readLine()) != null) {
					final String[] words = Utils.splitString(line, SEPARATOR_CHAR);
					totalWords += words.length;
				}

				in.close();

				results[i] = (System.nanoTime() - start_time) / 1000000000.0;

				//				System.out.println("total Words: "+totalWords);
			}

			return Utils.mean(results);
		}

		@Override
		public String toString() {
			return "BufferedReader";
		}

	}

	static class CharChannel extends AbstractStrategy {
		final boolean direct;
		final int numLines;

		CharChannel(final int N, final boolean direct, final int numLines) {
			super(N, true);

			this.direct = direct;
			this.numLines = numLines;
		}

		@Override
		public double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				final FileInputStream fis = new FileInputStream(path);
				final FileChannel fc = fis.getChannel();
				//TODO compare between allocate and allocateDirect
				final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufSize) : ByteBuffer.allocate(1024*bufSize);
				Charset encoding = Charset.defaultCharset();

				while (fc.read(bb) > 0) {
					bb.flip();

					encoding.decode(bb);

					bb.clear();
				}

				fc.close();
				fis.close();

				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}

			return Utils.mean(results);
		}

		@Override
		public String toString() {
			return "Char Channel" + (direct ? " with direct allocation":"");
		}

	}

	static class ByteBufferLoad extends AbstractStrategy {
		final boolean direct;
		final int numLines;

		int min_numBytes;

		ByteBufferLoad(final int N, final boolean direct, final int numLines) {
			super(N, true);
			this.direct = direct;
			this.numLines = numLines;
		}

		@Override
		public double load(String path, int bufSize) throws Exception {

			final double results[] = new double[N];

			// start with get()
			for (int i = 0; i < N; i++) {
				results[i] = internalLoad(path, bufSize);
			}
			double min_mean = Utils.mean(results);
			min_numBytes = 0;

			for (int numBytes = 1; numBytes < bufSize; numBytes *= 2) {
				for (int i = 0; i < N; i++) {
					results[i] = internalBufferLoad(path, bufSize, numBytes);
				}
				final double mean = Utils.mean(results);
				if (mean < min_mean) {
					min_mean = mean;
					min_numBytes = numBytes;
				}
			}

			// finish with numBytes = bufSize
			for (int i = 0; i < N; i++) {
				results[i] = internalBufferLoad(path, bufSize, bufSize);
			}
			final double mean = Utils.mean(results);
			if (mean < min_mean) {
				min_mean = mean;
				min_numBytes = bufSize;
			}

			return min_mean;
		}

		double internalLoad(final String path, final int bufSize) throws Exception {

			final long start_time = System.nanoTime();

			SeekableByteChannel fc = Files.newByteChannel(Paths.get(path));

			final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufSize) : ByteBuffer.allocate(1024*bufSize);

			while (fc.read(bb) > 0) {
				bb.flip();

				for (int i = 0; i < bb.limit(); i++) {
					final byte b = bb.get();
				}

				bb.clear();
			}

			fc.close();

			return (System.nanoTime() - start_time) / 1000000000.0;
		}

		double internalBufferLoad(final String path, final int bufSize, final int numBytes) throws Exception {

			final long start_time = System.nanoTime();

			SeekableByteChannel fc = Files.newByteChannel(Paths.get(path));

			final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufSize) : ByteBuffer.allocate(1024*bufSize);

			final byte[] bytes = new byte[1024*numBytes];

			while (fc.read(bb) > 0) {
				bb.flip();

				int offset = 0;
				int avail = bb.limit();

				while (offset < avail) {
					if ((offset + bytes.length) > avail) {
						bb.get(bytes, 0, avail - offset);
						offset = avail;
					}
					else {
						bb.get(bytes);
						offset += bytes.length;
					}
				}

				bb.clear();
			}

			fc.close();

			return (System.nanoTime() - start_time) / 1000000000.0;
		}

		@Override
		public String toString() {
			return "ByteBuffer Channel" + (direct ? " with direct allocation":"");
		}

		@Override
		public String describeBest() {
			return " with numBytes = "+min_numBytes;
		}

	}

	static class ByteParser extends AbstractStrategy {
		final int numLines;
		final boolean direct;

		ByteParser(final int N, final boolean direct, final int numLines) {
			super(N, true);
			this.direct = direct;
			this.numLines = numLines;
		}

		@Override
		public double load(String path, int bufSize) throws Exception {

			final double results[] = new double[N];

			// finish with numBytes = bufSize
			for (int i = 0; i < N; i++) {
				results[i] = internalLoad(path, bufSize);
			}

			return Utils.mean(results);
		}

		double internalLoad(final String path, final int bufSize) throws Exception {

			final long start_time = System.nanoTime();

			SeekableByteChannel fc = Files.newByteChannel(Paths.get(path));
			final int size = (int) fc.size(); // we'll be in trouble if the file is too big

			final ByteBuffer bb = direct ? ByteBuffer.allocateDirect(1024*bufSize) : ByteBuffer.allocate(1024*bufSize);

			final byte[] data = new byte[size];
			int curIdx = 0;

			while (fc.read(bb) > 0) {
				bb.flip();

				bb.get(data, curIdx, bb.limit());
				curIdx += bb.limit();

				bb.clear();
			}

			fc.close();

			return (System.nanoTime() - start_time) / 1000000000.0;
		}

		@Override
		public String toString() {
			return "ByteBuffer Parser" + (direct ? " with direct allocation":"");
		}

	}
}

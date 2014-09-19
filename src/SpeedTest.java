import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;


/**
 * Simple tool to try various file reading strategies and compute average speed
 * 
 */
public class SpeedTest {

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

		System.out.println("Counting lines...");
		final int numLines = countNumLines(path);
		System.out.printf("File contains %d lines \n", numLines);
		
		final AbstractStrategy[] strategies = {
				new BufferLoad(N),
				new CharChannel(N, false, numLines),
//				new CharChannel(N, true, numLines),
				new AntlrLoad(N, false),
//				new AntlrLoad(N, true),
				new ByteBufferLoad(N, false, numLines),
				new ByteBufferLoad(N, true, numLines),
				new ByteParser(N, numLines)
		};

		for (AbstractStrategy strategy : strategies) {
			System.out.println("Loading File using "+strategy);

			double min_mean = Double.MAX_VALUE;
			int min_bufSize = 0;

			for (int bufSize : bufSizes) {
				final double mean = strategy.load(path, bufSize);
				if (mean < min_mean) {
					min_mean = mean;
					min_bufSize = bufSize;
				}
			}
			displayAvgSpeed("Best Speed for bufSize = "+min_bufSize + strategy.describeBest(), min_mean, numLines);
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
		
		AbstractStrategy(final int N) {
			this.N = N;
		}
		
		abstract double load(final String path, final int bufSize) throws Exception;

		public String describeBest() {
			return "";
		}

	}

	static class AntlrLoad extends AbstractStrategy {
		final boolean parse;
		
		AntlrLoad(final int N, final boolean parse) {
			super(N);
			this.parse = parse;
		}
		
		@Override
		public double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];
			
			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

//				final FileInputStream fis = new FileInputStream(path);
//				UnbufferedCharStream input = new UnbufferedCharStream(fis, 1024*bufSize); 
				ANTLRFileStream input = new ANTLRFileStream(path);
/*
				// create and instance of the lexer
				SCSVLexer lexer = new SCSVLexer(input);

				// wrap a token-stream around the lexer

				if (parse) {
					CommonTokenStream tokens = new CommonTokenStream(lexer);

					// create the parser
					SCSVParser parser = new SCSVParser(tokens);
					parser.setBuildParseTree(false); // useful for large files

					// invoke the entry point of our grammar
					parser.file();
				} else {
					// get all tokens
					while (lexer.nextToken().getType() != Token.EOF) {
					}
				}
*/				
				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}

			return StdStats.mean(results);
		}

		@Override
		public String toString() {
			return parse ? "ANTLR CSV Parser" : "ANTLR CSV Lexer";
		}
	}

	static class BufferLoad extends AbstractStrategy {

		BufferLoad(final int N) {
			super(N);
		}
		
		@Override
		public double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];
			
			for (int i = 0; i < N; i++) {
				final long start_time = System.nanoTime();

				final InputStream is = new FileInputStream(path);
				final BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), bufSize*1024);

				while (in.readLine() != null) {
				}

				in.close();

				results[i] = (System.nanoTime() - start_time) / 1000000000.0;
			}
			
			return StdStats.mean(results);
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
			super(N);
			
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
			
			return StdStats.mean(results);
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
			super(N);
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
			double min_mean = StdStats.mean(results);
			min_numBytes = 0;

			for (int numBytes = 1; numBytes < bufSize; numBytes *= 2) {
				for (int i = 0; i < N; i++) {
					results[i] = internalBufferLoad(path, bufSize, numBytes);
				}
				final double mean = StdStats.mean(results);
				if (mean < min_mean) {
					min_mean = mean;
					min_numBytes = numBytes;
				}
			}

			// finish with numBytes = bufSize
			for (int i = 0; i < N; i++) {
				results[i] = internalBufferLoad(path, bufSize, bufSize);
			}
			final double mean = StdStats.mean(results);
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
					bb.get();
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

		int min_numBytes;

		ByteParser(final int N, final int numLines) {
			super(N);
			this.numLines = numLines;
		}

		@Override
		public double load(String path, int bufSize) throws Exception {

			final double results[] = new double[N];

			for (int i = 0; i < N; i++) {
				results[i] = internalLoad(path, bufSize);
			}

			return StdStats.mean(results);
		}

		double internalLoad(final String path, final int bufSize) throws Exception {

			final long start_time = System.nanoTime();

			SeekableByteChannel fc = Files.newByteChannel(Paths.get(path));

			final ByteBuffer bb = ByteBuffer.allocateDirect(1024*bufSize);
			
			final byte[] line = new byte[1024];
			final byte nl = (byte)'\n';
			int len = 0;
			
			while (fc.read(bb) > 0) {
				bb.flip();

				final int avail = bb.limit();
				for (int i = 0; i < avail; i++) {
					final byte b = bb.get();
					if (b == nl) {
						len = 0;
					} else {
						line[len++] = b;						
					}
				}

				bb.clear();
			}

			fc.close();

			return (System.nanoTime() - start_time) / 1000000000.0;
		}

		@Override
		public String toString() {
			return "ByteBuffer Parser";
		}

	}
}

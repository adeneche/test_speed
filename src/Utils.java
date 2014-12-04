import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.hbase.async.Bytes;


public class Utils {

	/** Mask to verify a timestamp on 4 bytes in seconds */
	public static final long SECOND_MASK = 0xFFFFFFFF00000000L;

	public static class TimeValue {
		
		public static void fromByteArray(final byte[] bytes, final int off, final TimeValue dp) {
//			int cur = off * TimeValue.SIZE;
//			dp.mtsIdx = Bytes.getShort(bytes, cur); cur+= 2;
//			dp.timestamp = Bytes.getLong(bytes, cur); cur+= 8;
//			dp.ivalue = Bytes.getInt(bytes, cur); cur+= 4;

			//return new TimeValue(mtsIdx, timestamp, ivalue);
		}

		public static int toByteArray(final byte[] bytes, final int offset, final short mtsIdx, final long timestamp, final byte[] value) {
			int cur = offset;
			bytes[cur++] = (byte) (11 + value.length);
			System.arraycopy(Bytes.fromShort(mtsIdx), 0, bytes, cur, 2); cur+= 2;
			System.arraycopy(Bytes.fromLong(timestamp), 0, bytes, cur, 8); cur+= 8;
			System.arraycopy(value, 0, bytes, cur, value.length); cur+= value.length;
			
			return 11 + value.length;
		}
	}
	
	public static class MetricTags {
		private final int metric;
		private final List<Integer> tagks;
		private final List<Integer> tagvs;

		public MetricTags(final byte[] bytes, final int offset, final int to) {
			this.metric = addMtsValue(bytes, offset, to);
			tagks = new ArrayList<Integer>();
			tagvs = new ArrayList<Integer>();
		}
		
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return Objects.hash(metric, tagks, tagvs);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) return true;
			if (obj == null || !(obj instanceof MetricTags)) return false;
			
			MetricTags mts = (MetricTags) obj;
			
			return metric == mts.metric && tagks.equals(mts.tagks) && tagvs.equals(mts.tagvs);
		}

		public void put(final byte[] bytes, final int tagk_offset, final int tagk_to, final int tagv_offset, final int tagv_to) {
			//				if (kv[1].equals(tags.get(kv[0]))) {
			//					return;
			//				}
			//				if (tags.get(kv[0]) != null) {
			//					throw new IllegalArgumentException("duplicate tag: " + tag
			//							+ ", tags=" + tags);
			//				}
			tagks.add(addMtsValue(bytes, tagk_offset, tagk_to));
			tagvs.add(addMtsValue(bytes, tagv_offset, tagv_to));
		}
	}

	public static final List<MetricTags> metricTags = new ArrayList<MetricTags>();
	public static final List<byte[]> mtsValues = new ArrayList<byte[]>();


	public static double mean(double[] values) {
		double sum = 0;
		for (double value : values) {
			sum += value;
		}
		
		return sum / values.length;
	}

	private static int addMtsValue(final byte[] bytes, final int offset, final int to) {
		final int length = to - offset;
mainloop:
		for (int idx = 0; idx < mtsValues.size(); idx++) {
			final byte[] value = mtsValues.get(idx);
	        
	        if (length != value.length) continue;

	        for (int i=0; i<length; i++)
	            if (bytes[i+offset] != value[i]) continue mainloop;

	        return idx;
		}

		mtsValues.add(Arrays.copyOfRange(bytes, offset, to));
		return mtsValues.size() - 1;
	}
	
	public static double parseBytes(final byte[] bytes) {
		final byte _r = '\r';
		final byte _n = '\n';
		final byte _equal = '=';
		final byte _space = SpeedTest.SEPARATOR_CHAR;

		int numlines = 0;

//		int lastDPIdx = 0;
		double totalWords = 0;
		
		int startIdx = 0;
		int pos = 0;

		int state = 0;
		int equalPos = -1; // last _equal char found
		MetricTags mts = null;
		long timestamp = -1;
		byte[] value = null;
		
		for (final byte b : bytes) {
			if (b == _space || b == _r || b == _n || pos == bytes.length) {
				if (startIdx < pos) { // we can actually parse this word bytes[startIdx, pos[
					final int size = pos - startIdx;
					switch (size) {
					case 1:
						totalWords += bytes[startIdx] - '0';
						break;
					case 2:
						totalWords += (bytes[startIdx] - '0') * 10 + bytes[startIdx + 1] - '0';
						break;
					default:
						double r = 0;
						for (int i = 0; i < size; i++) {
							r = 10 * r + bytes[startIdx + i] - '0';
						}
						totalWords += r;
					}

//					switch (state) {
//						case 0: // metric
//							mts = new MetricTags(bytes, startIdx, pos);
//							break;
//						case 1: // timestamp
//							try {
//								timestamp = parseLong(bytes, startIdx, pos-startIdx);
//							} catch (NumberFormatException e) {
//								throw new IllegalArgumentException("invalid timestamp at line " + numlines);
//							}
//							break;
//						case 2: // value
//							value = Arrays.copyOfRange(bytes, startIdx, pos);
//							break;
//						default: // tagk=tagv
//							if (equalPos == -1 || equalPos == startIdx || equalPos == pos-1) {
//								throw new IllegalArgumentException("invalid tag at line " + numlines);
//							}
//
//							mts.put(bytes, startIdx, equalPos, equalPos+1, pos);
//					}
//
//					state++;
//					totalWords++;
				}

				startIdx = pos + 1; // new word
				equalPos = -1; // equalPos points to the last equal sign found in the current word

				if (b != _space) { // new line or end of file
//					if (state < 3) {
//						throw new IllegalArgumentException("line "+numlines+" is incomplete");
//					}
/*
					int mtsIdx = metricTags.indexOf(mts);
					if (mtsIdx == -1) {
						mtsIdx = metricTags.size();
						metricTags.add(mts);
					}

					// store current datapoint
					lastDPIdx += TimeValue.toByteArray(bytes, lastDPIdx, (short) mtsIdx, timestamp, value);
*/					
					state = 0;
					mts = null;
					value = null;
					numlines++;
				}
			} else if (b == _equal) {
				if (equalPos != -1) {
					throw new IllegalArgumentException("invalid tag at line " + numlines);
				}
				equalPos = pos;
			}

			pos++;
		}
		
//		return lastDPIdx;
		return totalWords;
	}
	
	public static String bytes2str(final byte[] bytes) {
		return bytes2str(bytes, 0, bytes.length);
	}

	public static String bytes2str(final byte[] bytes, final int offset, final int length) {
		return new String(bytes, offset, length, StandardCharsets.UTF_8);
	}

	private static final char[] chars = new char[100];
	

	public static String[] splitString(final byte[] bytes, final int startIdx, final int length, final byte c) {
		int num_substrings = 1;
		for (int i = 0; i <  length; i++) {
			if (bytes[i+startIdx] == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];

		int start = 0;  // starting index in bytes of the current substring.
		int pos = 0;    // current index in bytes.
		int i = 0;      // number of the current substring.
		for (; pos < length; pos++) {
			if (bytes[pos+startIdx] == c) {
				for (int p = start; p < pos; p++) chars[p-start] = (char) bytes[p+startIdx];
				result[i++] = new String(chars, 0, pos - start);
				start = pos + 1;
			}
		}
		for (int p = start; p < pos; p++) chars[p-start] = (char) bytes[p+startIdx];
		result[i] = new String(chars, 0, pos - start);

		return result;
	}

	/**
	 * Optimized version of {@code String#split} that doesn't use regexps.
	 * This function works in O(5n) where n is the length of the string to
	 * split.
	 * @param s The string to split.
	 * @param c The separator to use to split the string.
	 * @return A non-null, non-empty array.
	 */
	public static String[] splitString(final String s, final char c) {
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		for (final char x : chars) {
			if (x == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++) {
			if (chars[pos] == c) {
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		return result;
	}

	/**
	 * Parses an integer value as a long from the given character sequence.
	 * <p>
	 * This is equivalent to {@link Long#parseLong(String)} except it's up to
	 * 100% faster on {@link String} and always works in O(1) space even with
	 * {@link StringBuilder} buffers (where it's 2x to 5x faster).
	 * @param s The character sequence containing the integer value to parse.
	 * @return The value parsed.
	 * @throws NumberFormatException if the value is malformed or overflows.
	 */
	public static long parseLong(final CharSequence s) {
		final int n = s.length();  // Will NPE if necessary.
		if (n == 0) {
			throw new NumberFormatException("Empty string");
		}
		char c = s.charAt(0);  // Current character.
		int i = 1;  // index in `s'.
		if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
			if (n == 1) {
				throw new NumberFormatException("Just a sign, no value: " + s);
			} else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
				throw new NumberFormatException("Value too long: " + s);
			}
			c = s.charAt(1);
			i = 2;  // Skip over the sign.
		} else if (n > 19) {  // "9223372036854775807"
			throw new NumberFormatException("Value too long: " + s);
		}
		long v = 0;  // The result (negated to easily handle MIN_VALUE).
		do {
			if ('0' <= c && c <= '9') {
				v -= c - '0';
			} else {
				throw new NumberFormatException("Invalid character '" + c
						+ "' in " + s);
			}
			if (i == n) {
				break;
			}
			v *= 10;
			c = s.charAt(i++);
		} while (true);
		if (v > 0) {
			throw new NumberFormatException("Overflow in " + s);
		} else if (s.charAt(0) == '-') {
			return v;  // Value is already negative, return unchanged.
		} else if (v == Long.MIN_VALUE) {
			throw new NumberFormatException("Overflow in " + s);
		} else {
			return -v;  // Positive value, need to fix the sign.
		}
	}

	public static long parseLong(final byte[] bytes, final int offset, final int length) {
		final byte _zero = '0';
		final byte _nine = '9';
		final byte _plus = '+';
		final byte _minus = '-';
		
		final int n = length;  // Will NPE if necessary.
		if (n == 0) {
			throw new NumberFormatException("Empty string");
		}
		
		byte c = bytes[offset];  // Current character.
		int i = 1;  // index in `s'.
		if (c < _zero && (c == _plus || c == _minus)) {  // Only 1 test in common case.
			if (n == 1) {
				throw new NumberFormatException("Just a sign, no value");
			} else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
				throw new NumberFormatException("Value too long");
			}
			c = bytes[offset+1];
			i = 2;  // Skip over the sign.
		} else if (n > 19) {  // "9223372036854775807"
			throw new NumberFormatException("Value too long");
		}
		long v = 0;  // The result (negated to easily handle MIN_VALUE).
		do {
			if (_zero <= c && c <= _nine) {
				v -= c - _zero;
			} else {
				throw new NumberFormatException("Invalid character '" + c + "'");
			}
			if (i == n) {
				break;
			}
			v *= 10;
			c = bytes[offset + (i++)];
		} while (true);
		if (v > 0) {
			throw new NumberFormatException("Overflow");
		} else if (bytes[offset] == _minus) {
			return v;  // Value is already negative, return unchanged.
		} else if (v == Long.MIN_VALUE) {
			throw new NumberFormatException("Overflow");
		} else {
			return -v;  // Positive value, need to fix the sign.
		}
	}

	/**
	 * Parses a tag into a HashMap.
	 * @param tags The HashMap into which to store the tag.
	 * @param tag A String of the form "tag=value".
	 * @throws IllegalArgumentException if the tag is malformed.
	 * @throws IllegalArgumentException if the tag was already in tags with a
	 * different value.
	 */
	public static void parse(final HashMap<String, String> tags,
			final String tag) {
		final String[] kv = splitString(tag, '=');
		if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
			throw new IllegalArgumentException("invalid tag: " + tag);
		}
		if (kv[1].equals(tags.get(kv[0]))) {
			return;
		}
		if (tags.get(kv[0]) != null) {
			throw new IllegalArgumentException("duplicate tag: " + tag
					+ ", tags=" + tags);
		}
		tags.put(kv[0], kv[1]);
	}

	/**
	 * Returns true if the given string looks like an integer.
	 * <p>
	 * This function doesn't do any checking on the string other than looking
	 * for some characters that are generally found in floating point values
	 * such as '.' or 'e'.
	 * @since 1.1
	 */
	public static boolean looksLikeInteger(final String value) {
		final int n = value.length();
		for (int i = 0; i < n; i++) {
			final char c = value.charAt(i);
			if (c == '.' || c == 'e' || c == 'E') {
				return false;
			}
		}
		return true;
	}
}

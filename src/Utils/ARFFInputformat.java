package Utils;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * ARFF file hadoop input file format
 * 
 * 
 */
public class ARFFInputformat extends FileInputFormat<Text, SparseVector> {

	private final static Pattern p = Pattern.compile("([^,]+,)|([^,]+})");

	static class Reader extends RecordReader<Text, SparseVector> {

		private Text key;
		private SparseVector value;
		private final LineRecordReader r;
		private long start;

		public Reader() {
			r = new LineRecordReader();
		}

		@Override
		public void close() throws IOException {
			r.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public SparseVector getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return r.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			r.initialize(split, context);
			FileSplit fs = (FileSplit) split;
			start = fs.getStart();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (r.nextKeyValue()) {
				Text line = r.getCurrentValue();
				String str = "1";
				Vector2<String, SparseVector, String> v = readLineTrain(start,
						line.toString(), str);
				key = new Text(v.getV1());
				value = v.getV2();
				start += line.getLength();
				return true;
			}
			return false;
		}
	}
	public static Vector2<String, SparseVector, String> readLineTrain(
			long start, String line, String str) {

		// offset as ID

		// read value
		SparseVector value = new SparseVector();
		String str1 = null;
		String key = null;

		int i = 1;
		String[] k = line.split(",");
		key = k[k.length - 1];
		for (String s : line.split(",")) {
			s = s.trim();
			if (!Character.isDigit(s.charAt(0))) {

			} else {
				value.put(i + "", Float.parseFloat(s));
				i++;
			}
		}
		return new Vector2<String, SparseVector, String>(key, value, str1);
	}
	
	public static Vector2<String, SparseVector, String> readLine(long start,
			String line, String str) {
		if (line.startsWith("{")) {
			// split the line into key and value
			// remove the round blanket
			Matcher m = p.matcher(line.toString());
			// load ID string
			m.find();
			String s = m.group();
			s = s.substring(0, s.length() - 1);
			// read the key
			str = "";
			String key = null;
			// String str = null;
			if (s.split(" ")[0].equals(SparseVector.ID)) {
				// if this file ignores the ID, we just use the file offset
				// instead
				key = s.split(" ")[1];
			} else {
				key = start + "";
			}
			// read value
			SparseVector value = new SparseVector();
			while (m.find()) {
				s = m.group();
				s = s.substring(0, s.length() - 1);
				String c = s.split(" ")[0];
				float v = Float.parseFloat(s.split(" ")[1]);
				// System.out.println(v);
				value.put(c, v);
			}
			return new Vector2<String, SparseVector, String>(key, value, str);
		} else {
			// offset as ID
			String key = start + "";
			// read value
			SparseVector value = new SparseVector();
			// String str = null;
			int i = 1;
			for (String s : line.split(",")) {
				s = s.trim();
				if (!Character.isDigit(s.charAt(0))) {
					
					
				} else {
					value.put(i + "", Float.parseFloat(s));
					i++;
					str += s+",";  
				}
			}
			str = str.substring(0, str.length()-1);
			
			return new Vector2<String, SparseVector, String>(key, value, str);
		}
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	@Override
	public RecordReader<Text, SparseVector> createRecordReader(
			final InputSplit split, final TaskAttemptContext context) {
		return new Reader();
	}
}
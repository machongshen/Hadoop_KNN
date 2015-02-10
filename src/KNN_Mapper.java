import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utils.*;
/**
 * @author machongshen
 */
public class KNN_Mapper
		extends
			Mapper<Text, SparseVector, IntWritable, Storage_Vector> {

	private Vector<KNN_Storage<String, SparseVector, String>> test = new Vector<KNN_Storage<String, SparseVector, String>>();
	private int count = 0;
	protected void map(
			Text key,
			SparseVector value,
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, IntWritable, Storage_Vector>.Context context)
			throws java.io.IOException, InterruptedException {

		context.setStatus(key.toString());
		for (KNN_Storage<String, SparseVector, String> testCase : test) {
			double d = testCase.getV2().euclideanDistance(value);
			String s = key.toString();
			String k = "";

			k += testCase.getV3() + ",";

			// System.out.println("k="+k);
			k = k.substring(0, k.length() - 1);
			// System.out.println("machongshen nihao ="+testCase.getV1().trim());
			context.write(new IntWritable(Integer.parseInt(testCase.getV1())),
					new Storage_Vector(s, (float) d, k));
			count++;
		}
		//System.out.println("count=" + count);
	}

	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, IntWritable, Storage_Vector>.Context context)
			throws java.io.IOException, InterruptedException {
		// test.close();
	}

	;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, IntWritable, Storage_Vector>.Context context)
			throws java.io.IOException, InterruptedException {
		// System.out.print("loading shared comparison vectors...");
		// load the test vectors
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(new Path(context.getConfiguration().get("test_data",
						"test.arff")))));
		String line = br.readLine();
		int count = 0;
		while (line != null) {
			String str = "";
			KNN_Storage<String, SparseVector, String> v = KNN_Inputformat.readLine(
					count, line, str);
			test.add(new KNN_Storage<String, SparseVector, String>(v.getV1(), v
					.getV2(), v.getV3()));
			line = br.readLine();
			count++;
		}
		br.close();
		System.out.println("done. " + count);
	};
}
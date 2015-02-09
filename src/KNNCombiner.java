import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.*;

public class KNNCombiner extends Reducer<IntWritable, Vector2SF, IntWritable, Vector2SF> {
	protected void reduce(
			IntWritable key,
			java.lang.Iterable<Vector2SF> value,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, Vector2SF, IntWritable, Vector2SF>.Context context)
			throws java.io.IOException, InterruptedException {
		LinkedList<Vector2SF> vs = new LinkedList<Vector2SF>();
		// sort each vector2SF by similarty
		for (Vector2SF v : value) {
			vs.add(new Vector2SF(v.getV1(), v.getV2(), v.getV3()));
			//System.out.println("V3=" + v.getV3());
		}
		
		//System.out.println("*****************************");
		
		for (int i = 0; i < vs.size(); i++) {
			
			context.write(key, vs.get(i));
			
		}
	};
}
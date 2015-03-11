import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.*;
/**
 * @author machongshen
 */
public class KNN_Combiner extends Reducer<IntWritable, Storage_Vector, IntWritable, Storage_Vector> {
	protected void reduce(
			IntWritable key,
			java.lang.Iterable<Storage_Vector> value,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, Storage_Vector, IntWritable, Storage_Vector>.Context context)
			throws java.io.IOException, InterruptedException {
		LinkedList<Storage_Vector> vs = new LinkedList<Storage_Vector>();
		// sort each vector2SF by similarty
		for (Storage_Vector v : value) {
			vs.add(new Storage_Vector(v.getV1(), v.getV2(), v.getV3()));
			//System.out.println("V3=" + v.getV3());
		}
		
		for (int i = 0; i < vs.size(); i++) {
			
			context.write(key, vs.get(i));
			
		}
	};
}
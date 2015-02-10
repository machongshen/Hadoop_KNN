import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.*;


/**
 * @author machongshen
 */
public class KNN_Reducer extends Reducer<IntWritable, Storage_Vector, Text, Text> {

	protected void reduce(
			IntWritable key,
			java.lang.Iterable<Storage_Vector> value,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, Storage_Vector, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		ArrayList<Storage_Vector> vs = new ArrayList<Storage_Vector>();
		// sort each vector2SF by similarty
		String s = "";
		for (Storage_Vector v : value) {
			vs.add(new Storage_Vector(v.getV1(), v.getV2(), v.getV3()));
			//System.out.println(v.getV3()+"adf" );
			s = v.getV3();
		}
		int k = context.getConfiguration().getInt("knn.k", 5);
	
		Collections.sort(vs, new Comparator<Storage_Vector>() {
			@Override
			public int compare(Storage_Vector o1, Storage_Vector o2) {
				if (o2.getV2() > o1.getV2()) {
					return -1;
				} else
					return 1;
			}
		});
		

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		HashMap<String, Float> mapvalue = new HashMap<String, Float>();
		
		for (int i = 0; i < k && i < vs.size(); i++) {
			
			if (map.containsKey(vs.get(i).getV1())) {
				map.put(vs.get(i).getV1(), map.get(vs.get(i).getV1()) + 1);
				mapvalue.put(vs.get(i).getV1(), mapvalue.get(vs.get(i).getV1())
						+ vs.get(i).getV2());
			} else
				map.put(vs.get(i).getV1(), 1);
				mapvalue.put(vs.get(i).getV1(), vs.get(i).getV2());
			}
		String max = "";
		int maxint = 0;
		int curint = 0;
		IntWritable pkd = new IntWritable(10); 
		if (key == pkd){
			System.out.println("good");
		}
		for (String sp1 : map.keySet()) {
			curint = map.get(sp1);
			//System.out.println("m3+"+);
			if (curint < maxint) {
				continue;
			} else if (curint > maxint) {
				maxint = curint;
				max = sp1;
			} else if (curint == maxint) {
				if (mapvalue.get(sp1) > mapvalue.get(max)) {
					max = sp1;				
				} else
					continue;
			}
		}
		// str = "1";
		max =max;
		//String ks = key.toString();
		context.write(new Text(s), new Text(max));
	};
}
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.*;

public class KNNReducer extends Reducer<Text, Vector2SF, Text, Text> {

    protected void reduce(
            Text key,
            java.lang.Iterable<Vector2SF> value,
            org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, Text, Text>.Context context)
            throws java.io.IOException, InterruptedException {
        ArrayList<Vector2SF> vs = new ArrayList<Vector2SF>();
        // sort each vector2SF by similarty
       
        for (Vector2SF v : value) {
            vs.add(new Vector2SF(v.getV1(), v.getV2(),v.getV3()));
           // System.out.println(v.getV1()+"adf" );
            //System.out.println("key"+ v.getV3());
        }
        
        Collections.sort(vs, new Comparator<Vector2SF>() {

            @Override
            public int compare(Vector2SF o1, Vector2SF o2) {
        	if (o2.getV2()>o1.getV2()){
		    return 0;
		}else
		    return -1;
            }
        });
        int k = context.getConfiguration().getInt("org.niubility.knn.k", 5);
      
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        HashMap<String, Float> mapvalue = new HashMap<String, Float>();
        for (int i = 0; i < k && i < vs.size(); i++) {
            //List<Float> list= new ArrayList<Float>();
            System.out.println(vs.get(i).getV1());
            if (map.containsKey(vs.get(i).getV1()))
            {	
        	
        	map.put(vs.get(i).getV1(), map.get(vs.get(i).getV1())+1);
        	mapvalue.put(vs.get(i).getV1(), mapvalue.get(vs.get(i).getV1())+vs.get(i).getV2());
            
            }else 
        	
        	map.put(vs.get(i).getV1(), 1);
            	mapvalue.put(vs.get(i).getV1(), vs.get(i).getV2());
           // sp.put(vs.get(i).getV1(), vs.get(i).getV2());
         //   str = vs.get(i).getV3();
        }
        String max = "";
        int maxint = 0;
        int curint = 0;
        for ( String sp1 : map.keySet())
        {	
            
            curint = map.get(sp1);
            
            if (curint < maxint){
        	continue;
            }
            else if (curint > maxint){
        	maxint = curint;
        	max = sp1;
            }else if (curint == maxint){
        	if (mapvalue.get(sp1)>mapvalue.get(max)){
        	    max = sp1;
        	    
        	}
        	else continue;
            }
   
        }
        //str = "1";
        
        context.write(key, new Text(max));
    }
    ;
}
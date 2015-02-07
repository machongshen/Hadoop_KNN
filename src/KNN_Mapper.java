import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import  Utils.*;



public class KNN_Mapper extends Mapper<Text, SparseVector, Text, Vector2SF> {

    private Vector<Vector2<String, SparseVector,String>> test =
            new Vector<Vector2<String, SparseVector, String>>();
    private int count = 0;
    protected void map(
            Text key,
            SparseVector value,
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
        // calculate the distance for each test sample with the training
        // data
	//System.out.println("key.toString()"+key.toString());
        context.setStatus(key.toString());
        for (Vector2<String, SparseVector, String> testCase : test) {
            double d = testCase.getV2().euclideanDistance(value);
            
          	String s =key.toString(); 
          	
            context.write(new Text(testCase.getV1()), new Vector2SF(s,(float)d, testCase.getV3()));
            count ++;
        }
        System.out.println("count="+count);
    }

    protected void cleanup(
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
//        test.close();
    }

    ;

    protected void setup(
            org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
            throws java.io.IOException, InterruptedException {
       // System.out.print("loading shared comparison vectors...");

        // load the test vectors
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get(
                "test_data", "test.arff")))));
        String line = br.readLine();
        int count = 0;
        while (line != null) {
            System.out.println("good"+line);
            String str = "";
            Vector2<String, SparseVector,String> v = ARFFInputformat.readLine(count, line,str);
           // System.out.println("good"+line);
            test.add(new Vector2<String, SparseVector,String>(v.getV1(), v.getV2(), v.getV3()));
            line = br.readLine();
            count++;
        }
        br.close();
        
        System.out.println("done. "+ count);
    }

    ;
}
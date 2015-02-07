import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import  Utils.*;
public class KNNDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KNNDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // config a job and start it
        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");
        //look at the test folder
        for (FileStatus fs : FileSystem.get(conf).listStatus(new Path(args[2]))) {
            conf.set("test_data", fs.getPath().toString());
            System.out.println("current test file"
                    + conf.get("org.niubility.learning.test"));
            
            Job job = new Job(conf, "KNN Classifier");
            job.setJarByClass(KNNDriver.class);

            job.setMapperClass(KNN_Mapper.class);
            job.setReducerClass(KNNReducer.class);
            job.setCombinerClass(KNNCombiner.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Vector2SF.class);
            job.setOutputValueClass(SparseVector.class);
            job.setInputFormatClass(ARFFInputformat.class);
            job.setOutputFormatClass(ARFFOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path out = new Path(args[1]);
           FileSystem.get(conf).delete(out, true);
            FileOutputFormat.setOutputPath(job, out);
            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //set the current testing file
            int res = job.waitForCompletion(true) ? 0 : 1;
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }
}

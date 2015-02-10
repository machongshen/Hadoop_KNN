package Utils;

import java.io.DataOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * @author machongshen
 */
public class KNN_OutputFormat extends TextOutputFormat<Text, Text> {

    protected static class Writer extends RecordWriter<Text, Text> {

        private LineRecordWriter<Text, Text> w;

        public Writer(DataOutputStream dos, String codec) {
            w = new LineRecordWriter<Text, Text>(dos, codec);
        }

        public Writer(DataOutputStream dos) {
            w = new LineRecordWriter<Text, Text>(dos);
        }

        public synchronized void write(Text key, Text value)
                throws IOException {
            StringBuffer sb = new StringBuffer();
           // SortedMap<String, Float> map = new TreeMap<String, Float>(value);
//            for (String col : map.keySet()) {
//                sb.append(col + " " + map.get(col) + ",");
//            }
            // remove the "," at the ending
            w.write(new Text( key), new Text(value ));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            w.close(context);
        }

        ;
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(
                "separator", "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                    job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
                    conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new Writer(fileOut, keyValueSeparator);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new Writer(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }
}
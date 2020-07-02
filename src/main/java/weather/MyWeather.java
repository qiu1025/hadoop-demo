package weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyWeather {

        public static void main(String[] args) throws Exception {
//    public void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWeather.class);

        //-----job-------Map
//        job.setInputFormatClass();
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(TPartitioner.class);
        job.setSortComparatorClass(TSortComparator.class);
        //    job.setCombinerClass(TCombiner.class);
        //-----Map----end


        //-----reduce-----
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);
        //---- reduce end -----
        Path input = new Path("/data/qf/input");
        FileInputFormat.addInputPath(job, input);


        Path output = new Path("/data/qf/output");
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(2);
        // -----
        job.waitForCompletion(true);
    }
}
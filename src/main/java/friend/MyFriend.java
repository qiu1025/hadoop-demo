package friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFriend {

    public static void main(String[] args) throws Exception {
        Configuration configurable = new Configuration(true);
        Job job = Job.getInstance(configurable);
        job.setJarByClass(MyFriend.class);

        job.setMapperClass(FMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(FReducer.class);

        Path input = new Path("/data/qf/friend/input");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/data/qf/friend/output");
        if (output.getFileSystem(configurable).exists(output)) {
            output.getFileSystem(configurable).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);


        job.waitForCompletion(true);
    }
}

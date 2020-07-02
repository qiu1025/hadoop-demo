package weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<Weather, IntWritable> {

    @Override
    public int getPartition(Weather weather, IntWritable intWritable, int i) {

        return weather.hashCode() % i;
    }
}

package friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text mKey = new Text();
    IntWritable mVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = StringUtils.split(value.toString(), ' ');

        for (int i = 1; i < strings.length; i++) {
            mKey.set(getFriend(strings[0], strings[i]));
            mVal.set(0);
            context.write(mKey, mVal);

            for (int j = i+1;j<strings.length;j++){
                mKey.set(getFriend(strings[i], strings[j]));
                mVal.set(1);
                context.write(mKey, mVal);
            }
        }
    }


    public static String getFriend(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }
        return s2 + ":" + s1;
    }
}

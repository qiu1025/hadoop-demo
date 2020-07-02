package weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jboss.netty.util.internal.StringUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {


    Weather mKey = new Weather();
    IntWritable mVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] strs = StringUtil.split(value.toString(), '\t');
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            mKey.setYear(cal.get(Calendar.YEAR));
            mKey.setMonth(cal.get(Calendar.MONDAY) + 1);
            mKey.setDay(cal.get(Calendar.DAY_OF_MONTH));

            int wd = Integer.parseInt(strs[1].substring(0, strs[1].length() - 1));
            mKey.setWd(wd);
            mVal.set(wd);
            context.write(mKey, mVal);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}

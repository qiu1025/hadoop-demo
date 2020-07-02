package weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupingComparator extends WritableComparator {

    public TGroupingComparator() {
        super(Weather.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Weather weather1 = (Weather) a;
        Weather weather2 = (Weather) b;

        int c1 = Integer.compare(weather1.getYear(), weather2.getYear());
        if (c1 == 0) {
            return Integer.compare(weather1.getMonth(), weather2.getMonth());
        }
        return c1;
    }
}

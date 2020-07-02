package weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TSortComparator extends WritableComparator {
    public TSortComparator() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Weather weather1 = (Weather) a;
        Weather weather2 = (Weather) b;

        int c1 = Integer.compare(weather1.getYear(), weather2.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(weather1.getMonth(), weather2.getMonth());
            if (c2 == 0) {
                return -Integer.compare(weather1.getWd(), weather2.getWd());
            } return c2;
        }
        return c1;
    }
}

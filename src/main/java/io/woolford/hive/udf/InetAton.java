package io.woolford.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Description(
        name="InetAton",
        value="returns the integer value of an IPv4 address",
        extended="SELECT inet_aton(ip_address) from some_table limit 1;"
)
public class InetAton extends UDF {

    private LongWritable result = new LongWritable();

    public LongWritable evaluate(Text ip) {
        if (ip == null) {
            return null;
        }
        long ipres = -1;
        String ipstr = ip.toString();
        String[] strs = ipstr.split("\\.");
        if (strs.length != 4) {
            return null;
        }

        try {
            long x1 = Integer.parseInt(strs[0]);
            int x2 = Integer.parseInt(strs[1]);
            int x3 = Integer.parseInt(strs[2]);
            int x4 = Integer.parseInt(strs[3]);
            if (x1 < 0 || x1 > 255 || x2 < 0 || x2 > 255 || x3 < 0 || x3 > 255
                    || x4 < 0 || x4 > 255) {
                return null;
            }
            ipres = 0;
            ipres = (x1 & 0x0ff) << 24 | (x2 & 0x0ff) << 16 | (x3 & 0x0ff) << 8
                    | (x4 & 0x0ff);
            result.set(ipres);
            return result;

        } catch (NumberFormatException e) {
            return null;
        }
    }
}


import io.woolford.hive.udf.InetAton;
import junit.framework.Assert;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class InetAtonTest {

    @Test
    public void testUdfCase1() {
        InetAton example = new InetAton();
        Assert.assertEquals(new LongWritable(Long.valueOf("3232235777")), example.evaluate(new Text("192.168.1.1")));
    }

    @Test
    public void testUdfCase2() {
        InetAton example = new InetAton();
        Assert.assertNull(example.evaluate(new Text("799.799.799.799")));
    }

    @Test
    public void testUDFNullCheck() {
        InetAton example = new InetAton();
        Assert.assertNull(example.evaluate(null));
    }
}
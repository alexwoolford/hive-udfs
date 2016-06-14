import io.woolford.hive.udf.HaversineDistance;
import junit.framework.Assert;
import org.junit.Test;

public class HaversineDistanceTest {

    @Test
    public void testUdfCase1() {
        HaversineDistance example = new HaversineDistance();
        Assert.assertEquals(new Double("29.53267801988892"), example.evaluate(new Double("39.7392"), new Double("-104.9903"), new Double("39.9936"), new Double("-105.0897")));
    }

    @Test
    public void testUdfCase2() {
        HaversineDistance example = new HaversineDistance();
        Assert.assertNull(example.evaluate(null, new Double("-104.9903"), new Double("39.9936"), new Double("-105.0897")));
    }

}

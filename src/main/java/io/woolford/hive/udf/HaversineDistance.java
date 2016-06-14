package io.woolford.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name="HaversineDistance",
        value="returns the distance, in kilometers, between two lat/long coordinates",
        extended="SELECT haversine_distance(lat1, lon1, lat2, lon2) from some_table limit 1;"
)
public class HaversineDistance extends UDF {

    private static final int EARTH_RADIUS = 6371; // Approx Earth radius in KM

    public Double evaluate(Double startLat, Double startLong, Double endLat, Double endLong) {

        // if any of the input variables are null, then return null
        if (startLat == null || startLong == null || endLat == null || endLong == null ){
            return null;
        }

        Double distance;

        double dLat  = Math.toRadians((endLat - startLat));
        double dLong = Math.toRadians((endLong - startLong));

        startLat = Math.toRadians(startLat);
        endLat   = Math.toRadians(endLat);

        double a = haversine(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversine(dLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        distance = EARTH_RADIUS * c;

        return distance;
    }

    public static double haversine(double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }

}
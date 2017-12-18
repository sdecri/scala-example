/**
 * BoundingBox.java
 */
package com.sdc.scala_example.geometry;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

/**
 * @author simone.decristofaro
 *         Apr 4, 2017
 */
public class BoundingBox implements Serializable {

    private static final long serialVersionUID = -4371995287661701333L;

    public static final String PARSE_PATTERN_VALUE = "^(-*\\d*(\\.\\d*)*,){3}(-*\\d*(\\.\\d*)*)$";
    private static final Pattern PARSE_PATTERN = Pattern.compile(PARSE_PATTERN_VALUE);

    private double minX;
    private double minY;
    private double maxX;
    private double maxY;

    /**
     * 
     */
    public BoundingBox() {
        super();
    }

    /**
     * @param minX
     * @param minY
     * @param maxX
     * @param maxY
     */
    public BoundingBox(double minX, double minY, double maxX, double maxY) {
        super();
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
    }

    public BoundingBox(Envelope envelope) {
        this(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
    }

    public BoundingBox(BoundingBox boundingBox) {
        this(boundingBox.getMinX(), boundingBox.getMinY(), boundingBox.getMaxX(), boundingBox.getMaxY());
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null || !obj.getClass().equals(getClass()))
            return false;
        if (obj == this)
            return true;

        BoundingBox other = (BoundingBox) obj;

        return minX == other.minX && minY == other.minY && maxX == other.maxX && maxY == other.maxY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return String.format("FCD bounding box: BOTTOM_LEFT = [%s, %s]; UPPER_RIGHT = [%s, %s]", minX, minY, maxX, maxY);

    }

    public static BoundingBox merge(BoundingBox boundingBox1, BoundingBox boundingBox2) {

        if (boundingBox1 != null && boundingBox2 != null)
            return new BoundingBox(Math.min(boundingBox1.minX, boundingBox2.minX), Math.min(boundingBox1.minY, boundingBox2.minY),
                    Math.max(boundingBox1.maxX, boundingBox2.maxX), Math.max(boundingBox1.maxY, boundingBox2.maxY));
        else if (boundingBox1 == null && boundingBox2 != null) {
            return new BoundingBox(boundingBox2);
        }
        else if (boundingBox1 != null && boundingBox2 == null) {
            return new BoundingBox(boundingBox1);
        }
        else
            return null;
    }

    /**
     * Return <code>true</code> if the specified point is included in the {@link BoundingBox}
     * 
     * @param lon
     * @param lat
     * @return boolean
     */
    public boolean includes(double lon, double lat) {

        return (lon >= minX && lon <= maxX) && (lat >= minY && lat <= maxY);
    }

    /**
     * Return the x amplitude
     * 
     * @return double
     */
    public double getWidth() {

        return maxX - minX;
    }

    /**
     * Return the y amplitude
     * 
     * @return double
     */
    public double getHeight() {

        return maxY - minY;
    }

    public Polygon toPolygon() {

        return GeometryUtils.createPolygonFromBounds(minX, minY, maxX, maxY);
    }

    /**
     * Create a new {@link BoundingBox} apply a buffer on both sides (x and y) .
     * It implements a simpler solution than the {@link GeometryUtils#buffer(Geometry, double)} method,
     * that increases considerably the performances with an error of about 0.1 meter on 5 km buffer.
     * 
     * @param distance
     *            the distance in meters from both sides
     * @return if the buffer is negative the computed {@link BoundingBox}
     *         could be illogical
     */
    public BoundingBox buffer(double distance) {

        boolean isPositiveBuffer = distance > 0;

        double bufferRadiants = distance / (GeometryUtils.MEAN_EARTH_RADIUS * GeometryUtils.RADIANTS_OF_ONE_DEGREE);
        double xFactor = GeometryUtils.getXFactor(minY, maxY);

        int minFactor = isPositiveBuffer ? -1 : 1;
        double newMinX = minX * xFactor + bufferRadiants * minFactor;
        double newMinY = minY + bufferRadiants * minFactor;
        int maxFactor = isPositiveBuffer ? 1 : -1;
        double newMaxX = maxX * xFactor + bufferRadiants * maxFactor;
        double newMaxY = maxY + bufferRadiants * maxFactor;

        return new BoundingBox(newMinX / xFactor, newMinY, newMaxX / xFactor, newMaxY);
    }

    /**
     * @return the {@link BoundingBox#minX}
     */
    public double getMinX() {

        return minX;
    }

    /**
     * @param minX
     *            the {@link BoundingBox#minX} to set
     */
    public void setMinX(double minX) {

        this.minX = minX;
    }

    /**
     * @return the {@link BoundingBox#minY}
     */
    public double getMinY() {

        return minY;
    }

    /**
     * @param minY
     *            the {@link BoundingBox#minY} to set
     */
    public void setMinY(double minY) {

        this.minY = minY;
    }

    /**
     * @return the {@link BoundingBox#maxX}
     */
    public double getMaxX() {

        return maxX;
    }

    /**
     * @param maxX
     *            the {@link BoundingBox#maxX} to set
     */
    public void setMaxX(double maxX) {

        this.maxX = maxX;
    }

    /**
     * @return the {@link BoundingBox#maxY}
     */
    public double getMaxY() {

        return maxY;
    }

    /**
     * @param maxY
     *            the {@link BoundingBox#maxY} to set
     */
    public void setMaxY(double maxY) {

        this.maxY = maxY;
    }

    /**
     * Compute the external {@link BoundingBox} defined by the {@link List}
     * of {@link Geometry geometries}
     * @param geometries 
     * @return {@link BoundingBox}
     */
    public static BoundingBox fromGeometries(List<Geometry> geometries) {

        BoundingBox bbox = geometries.stream().map(geom -> {
            return new BoundingBox(geom.getEnvelopeInternal());
        }).reduce((previous, current) -> merge(previous, current)).orElse(null);

        return bbox;
    }

    public static BoundingBox fromCoordinates(List<Coordinate> coordinates) {

        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (Coordinate coordinate : coordinates) {
            if (minX > coordinate.x)
                minX = coordinate.x;
            if (minY > coordinate.y)
                minY = coordinate.y;
            if (maxX < coordinate.x)
                maxX = coordinate.x;
            if (maxY < coordinate.y)
                maxY = coordinate.y;
        }
        return new BoundingBox(minX, minY, maxX, maxY);
    }

    public static BoundingBox parse(String value) {

        if (!validatePattern(value))
            throw new IllegalArgumentException(
                    String.format("For input string: \"%s\". Regex validation is done against pattern: %s", value, PARSE_PATTERN_VALUE));

        
        
        
        String[] v = value.split(",");

        return new BoundingBox(Double.parseDouble(v[0]), Double.parseDouble(v[1]), Double.parseDouble(v[2]), Double.parseDouble(v[3]));
    }

    public static boolean validatePattern(String value) {

        Matcher matcher = PARSE_PATTERN.matcher(value);
        return matcher.find();
    }

}

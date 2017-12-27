package com.sdc.graphx_example.geometry;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.operation.buffer.BufferParameters;
import com.vividsolutions.jts.util.GeometricShapeFactory;

/**
 * Includes some useful geometric methods.
 * <br>
 * For more details about geographical computations see:
 * <a href=http://www.movable-type.co.uk/scripts/latlong.html>http://www.movable-type.co.uk/scripts/latlong.html</a>
 * 
 * @author simone.decristofaro 11/set/2014
 */
public class GeometryUtils {

    /** The Constant QUARTER_CIRCLE. */
    public static final int QUARTER_CIRCLE = 90;
    
    public static final int HALF_CIRCLE = 180;
    
    /** degree in a full circle */
    public static final int FULL_CIRCLE_DEGREE = 360;
    
    public static final double RADIANTS_OF_ONE_DEGREE = Math.PI/HALF_CIRCLE;
    
    /**
     * WGS84 defined equatorial radius (a) in meters
     */
    public static final double SEMI_MAJOR_AXIS = 6378137.0;
    
    /**
     * WGS84 defined reciprocal ellipsoid flattening (1/f)
     */
    public static final double RECIPROCAL_FLATTENING = 298.257223563;
    
    /**
     * WGS84 derived polar radius in meters (~ 6356752.314245179)
     */
    public static final double SEMI_MINOR_AXIS = SEMI_MAJOR_AXIS * (1-1/RECIPROCAL_FLATTENING);
    
    /**
     * WGS84 derived mean earth radius in meters ((2*a+b)/3 ~ 6371008.7714150598)
     */
    public static final double MEAN_EARTH_RADIUS = (2*SEMI_MAJOR_AXIS + SEMI_MINOR_AXIS) / 3;
    
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(PrecisionModel.maximumPreciseValue), 4326);
    public static final GeometricShapeFactory GEOMETRY_SHAPE_FACTORY = new GeometricShapeFactory(GEOMETRY_FACTORY);

    public static final Random RANDOM = new Random();
    
    private GeometryUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class");
    }
    
    public static double randomDouble(double min, double max) {

        return RANDOM.nextDouble() * (max - min) + min;
    }

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min
     *            Minimum value
     * @param max
     *            Maximum value. Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int randomInt(int min, int max) {

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = RANDOM.nextInt((max - min) + 1) + min;

        return randomNum;
    }

    /**
     * Rotate all bearings, given in degrees, to the interval [0,360[
     * 
     * @param b
     *            bearing in degrees
     * @return bearing in degrees, rotated to the interval [0,360[
     */
    public static double chopBearing(double b) {
        return (b % 360 + 360) % 360;
    }

    /**
     * Return the value to use to correct the longitude in order to accomplish plane operation using geographic
     * coordinate in WGS84.
     * 
     * @param latitudes
     *            latitudes of the point involved in the computation
     * @return double
     * @see <a href="http://www.movable-type.co.uk/scripts/latlong.html/">http://www.movable-type.co.uk/scripts/latlong.html</a>
     */
    public static double getXFactor(double... latitudes) {

        double sumY = 0;
        for (int i = 0; i < latitudes.length; i++) {
            sumY += latitudes[i];
        }
        double mean = sumY / latitudes.length;
        return Math.cos(mean * RADIANTS_OF_ONE_DEGREE);
    }

    /**
     * Return the value to use to correct the x-coordinate in order to
     * accomplish plane operation using geographic coordinate in WGS84.
     * 
     * @param points
     * @return double
     */
    public static double getXFactor(Coordinate... points) {

        double sumY = 0;
        for (int i = 0; i < points.length; i++) {
            sumY += points[i].y;
        }
        double mean = sumY / points.length;
        return Math.cos(mean * RADIANTS_OF_ONE_DEGREE);
    }

    /**
     * Return the value to use to correct the x-coordinate in order to
     * accomplish plane operation using geographic coordinate in WGS84.
     * 
     * @param points
     * @return double
     */
    public static double getXFactor(Point... points) {

        double sumY = 0;
        for (int i = 0; i < points.length; i++) {
            sumY += points[i].getY();
        }
        double mean = sumY / points.length;
        return Math.cos(mean * RADIANTS_OF_ONE_DEGREE);
    }

    /**
     * Return the dot product (in degree^2) between two vector AB, BC,
     * considering WGS84 geographic coordinates.<br>
     * AB * CD = (xB-xA)*(xD-xC)+(yB-yA)*(yD-yC)
     * 
     * @param xA
     *            X coordinate of the first point of the first vector
     * @param yA
     *            Y coordinate of the first point of the first vector
     * @param xB
     *            X coordinate of the second point of the first vector
     * @param yB
     *            Y coordinate of the second point of the first vector
     * @param xC
     *            X coordinate of the first point of the second vector
     * @param yC
     *            Y coordinate of the first point of the second vector
     * @param xD
     *            X coordinate of the second point of the second vector
     * @param yD
     *            Y coordinate of the second point of the second vector
     * @return double
     */
    public static double dotProduct(double xA, double yA, double xB, double yB, double xC, double yC, double xD, double yD) {

        double xFactor = getXFactor(new double[] { yA, yB, yC, yD });
        return dotProduct((xB - xA) * xFactor, (yB - yA), (xD - xC) * xFactor, (yD - yC));
    }

    /**
     * Return the dot product between two vector u, v.<br>
     * u * v = xu*xv+yu*yv
     * 
     * @param xu
     *            X component of the first vector
     * @param yu
     *            Y component of the first vector
     * @param xv
     *            X component of the second vector
     * @param yv
     *            Y component of the second vector
     * @return double
     */
    public static double dotProduct(double xu, double yu, double xv, double yv) {

        return (xu * xv + yu * yv);
    }

    /**
     * compute the angle between the two vectors (x1,y1) and (x2,y2), both starting from the origin
     * 
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @return double
     */
    public static double angleBetweenTwoVectors(double x1, double y1, double x2, double y2) {

        double modulesProduct = dotProduct(x1, y1, x1, y1) * dotProduct(x2, y2, x2, y2);

        if (modulesProduct > 0) {
            double argument = dotProduct(x1, y1, x2, y2) / Math.sqrt(modulesProduct);

            if (argument > 1) {
                return 0;
            }
            else if (argument < -1) {
                return Math.PI;
            }
            else {
                Math.acos(argument);
            }
        }
        else {
            return 0;
        }

        return 0;
    }

    /**
     * Compute the length in meters of a {@link LineString} with coordinate in
     * WGS84
     * @param line 
     * 
     * @return double
     */
    public static double getLength(LineString line) {

        double leng = 0;

        if (line != null) {
            Point p1 = null;
            Point p2 = null;
            for (int i = 0; i < line.getNumPoints() - 1; i++) {
                p1 = line.getPointN(i);
                p2 = line.getPointN(i + 1);
                leng += GeometryUtils.getDistance(p1, p2);
            }
        }
        return leng;
    }

    // POINT-POINT DISTANCE
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * JTS approximation
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return double
     */
    public static double getDistance(double lon1, double lat1, double lon2, double lat2) {

        return getDistanceJTS(lon1, lat1, lon2, lat2);
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * JTS approximation
     * 
     * @param p1
     * @param p2
     * @return double
     */
    public static double getDistance(Point p1, Point p2) {

        return getDistance(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * JTS approximation
     * 
     * @param p1
     * @param p2
     * @return double
     */
    public static double getDistance(Coordinate p1, Coordinate p2) {

        return getDistance(p1.x, p1.y, p2.x, p2.y);
    }

    public static double getBearing(double lon1, double lat1, double lon2, double lat2) {
        return getBearingJTS(lon1, lat1, lon2, lat2);
    }
    
    public static double getBearing(Coordinate c1, Coordinate c2) {

        return getBearing(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * TDE approximation
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return double
     */
    public static double getDistanceTDE(double lon1, double lat1, double lon2, double lat2) {

        double xFactor = Math.cos((lat1 + lat2) / 2 * RADIANTS_OF_ONE_DEGREE);
        double x = (lon2 - lon1) * xFactor;
        double y = (lat2 - lat1);
        return Math.sqrt(x * x + y * y) * RADIANTS_OF_ONE_DEGREE * MEAN_EARTH_RADIUS;
    }

    public static double getDistanceTDE(Coordinate c1, Coordinate c2) {
        return getDistanceTDE(c1.x, c1.y, c2.x, c2.y);
    }

    public static double getBearingTDE(double lon1, double lat1, double lon2, double lat2) {
        double xFactor = Math.cos((lat1 + lat2) / 2 * RADIANTS_OF_ONE_DEGREE);
        double x = (lon2 - lon1) * xFactor;
        double y = lat2 - lat1;
        double bearingRad = Math.atan2(x, y);
        double bearing = bearingRad / RADIANTS_OF_ONE_DEGREE;
        return chopBearing(bearing);
    }
    
    public static double getBearingTDE(Coordinate c1, Coordinate c2) {

        return getBearingTDE(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * <a href="http://mathworld.wolfram.com/SphericalTrigonometry.html">
     * spherical law of cosines</a>
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return double
     */
    public static double getDistanceCosLaw(double lon1, double lat1, double lon2, double lat2) {

        return Math.acos(Math.sin(lat1 * RADIANTS_OF_ONE_DEGREE) * Math.sin(lat2 * RADIANTS_OF_ONE_DEGREE) + Math.cos(lat1 * RADIANTS_OF_ONE_DEGREE)
                * Math.cos(lat2 * RADIANTS_OF_ONE_DEGREE) * Math.cos((lon2 - lon1) * RADIANTS_OF_ONE_DEGREE)) * MEAN_EARTH_RADIUS;
    }

    public static double getDistanceCosLaw(Coordinate c1, Coordinate c2) {
        return getDistanceCosLaw(c1.x, c1.y, c2.x, c2.y);
    }
    
    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * <a href="http://www.movable-type.co.uk/scripts/latlong.html">Haversine
     * law</a>
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return double
     */
    public static double getDistanceHaversine(double lon1, double lat1, double lon2, double lat2) {
        double lat1Rad = lat1 * RADIANTS_OF_ONE_DEGREE;
        double lat2Rad = lat2 * RADIANTS_OF_ONE_DEGREE;
        double deltaFI = (lat2 - lat1) * RADIANTS_OF_ONE_DEGREE / 2;
        double deltaLAMBDA = (lon2 - lon1) * RADIANTS_OF_ONE_DEGREE / 2;
        double sin_deltaFI = Math.sin(deltaFI);
        double sin_deltaLAMBDA = Math.sin(deltaLAMBDA);
        double a = sin_deltaFI * sin_deltaFI + Math.cos(lat1Rad) * Math.cos(lat2Rad) * sin_deltaLAMBDA * sin_deltaLAMBDA;
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return c * MEAN_EARTH_RADIUS;
    }

    public static double getDistanceHaversine(Coordinate c1, Coordinate c2) {
        return getDistanceHaversine(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * this, in fact, has nothing to do with "Haversine" whose name is abused
     * here to indicate that it's the bearing on the sphere
     * @param lon1 
     * @param lat1 
     * @param lon2 
     * @param lat2 
     * @return double
     */
    public static double getBearingHaversine(double lon1, double lat1, double lon2, double lat2) {
        double lat1Rad = lat1 * RADIANTS_OF_ONE_DEGREE;
        double lat2Rad = lat2 * RADIANTS_OF_ONE_DEGREE;
        double dLonRad = (lon2-lon1) * RADIANTS_OF_ONE_DEGREE;
        double y = Math.sin(dLonRad);
        double x = Math.cos(lat1Rad) * Math.tan(lat2Rad) 
                - Math.sin(lat1Rad) * Math.cos(dLonRad);
        double bearingRad = Math.atan2(y, x);
        double bearing = bearingRad / RADIANTS_OF_ONE_DEGREE;
        return chopBearing(bearing);
    }
    
    public static double getBearingHaversine(Coordinate c1, Coordinate c2) {

        return getBearingHaversine(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * <a href="http://en.wikipedia.org/wiki/Pythagorean_theorem">Pythagora's
     * theorem</a>. If performance is an issue and accuracy less important, for
     * small distances Pythagora's theorem can be used on an <a
     * href="http://en.wikipedia.org/wiki/Equirectangular_projection"
     * >equirectangular projection</a>:
     * 
     * @param lon1
     * @param lat1
     * @param lon2
     * @param lat2
     * @return double
     */
    public static double getDistancePythagora(double lon1, double lat1, double lon2, double lat2) {

        double lonRad = (lon2-lon1) * RADIANTS_OF_ONE_DEGREE;
        double latRad = (lat2-lat1) * RADIANTS_OF_ONE_DEGREE;
        double fimRad = (lat1 + lat2) * RADIANTS_OF_ONE_DEGREE/ 2;
        double cosFim = Math.cos(fimRad);
        double cosFim2 = cosFim * cosFim;
        double x2 = lonRad * lonRad * cosFim2;
        double y2 = latRad * latRad;
        return Math.sqrt(x2 + y2) * MEAN_EARTH_RADIUS;
    }

    public static double getDistancePythagora(Coordinate c1, Coordinate c2) {
        return getDistancePythagora(c1.x, c1.y, c2.x, c2.y);
    }

    public static double getBearingPythagora(double lon1, double lat1, double lon2, double lat2) {
        double fimRad = (lat1 + lat2) * RADIANTS_OF_ONE_DEGREE/ 2;
        double cosFim = Math.cos(fimRad);
        double bearingRad = Math.atan2((lon2-lon1) * cosFim, lat2-lat1);
        double bearing = bearingRad / RADIANTS_OF_ONE_DEGREE;
        return chopBearing(bearing);
    }
    
    public static double getBearingPythagora(Coordinate c1, Coordinate c2) {

        return getBearingPythagora(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * Compute distance in meters between two geographic point (WGS84) using the
     * <a href="http://www.vividsolutions.com/jts/JTSHome.htm">JTS</a>
     * functionalities
     * @param lon1 
     * @param lat1 
     * @param lon2 
     * @param lat2 
     * 
     * @return double
     */
    public static double getDistanceJTS(double lon1, double lat1, double lon2, double lat2) {

        double xFactor = getXFactor(new double[] { lat1, lat2 });
        Coordinate p1 = new Coordinate(0, lat1);
        Coordinate p2 = new Coordinate((lon2-lon1) * xFactor, lat2);
        return p1.distance(p2) * RADIANTS_OF_ONE_DEGREE * MEAN_EARTH_RADIUS;
    }

    public static double getDistanceJTS(Coordinate c1, Coordinate c2) {
        return getDistanceJTS(c1.x, c1.y, c2.x, c2.y);
    }

    public static double getBearingJTS(double lon1, double lat1, double lon2, double lat2) {
        double xFactor = getXFactor(new double[] { lat1, lat2 });
        double bearingRad = Math.atan2((lon2-lon1) * xFactor, (lat2-lat1));
        double bearing = bearingRad / RADIANTS_OF_ONE_DEGREE;
        return chopBearing(bearing);
    }
    
    public static double getBearingJTS(Coordinate c1, Coordinate c2) {
        return getBearingJTS(c1.x, c1.y, c2.x, c2.y);
    }

    /**
     * @param lon1 
     * @param lat1 
     * @param lon2 
     * @param lat2 
     * @return double
     * 
     */
    public static double getDistanceVB(double lon1, double lat1, double lon2, double lat2) {

        double xFactor = Math.cos(lat1 * RADIANTS_OF_ONE_DEGREE);
        Coordinate p1 = new Coordinate(0, lat1);
        Coordinate p2 = new Coordinate((lon2-lon1) * xFactor, lat2);
        return p1.distance(p2) * RADIANTS_OF_ONE_DEGREE * MEAN_EARTH_RADIUS;
    }

    public static double getDistanceVB(Coordinate c1, Coordinate c2) {
        return getDistanceVB(c1.x, c1.y, c2.x, c2.y);
    }

    public static double getBearingVB(double lon1, double lat1, double lon2, double lat2) {
        double xFactor = Math.cos(lat1 * RADIANTS_OF_ONE_DEGREE);
        double bearingRad = Math.atan2((lon2-lon1) * xFactor, lat2-lat1);
        double bearing = bearingRad / RADIANTS_OF_ONE_DEGREE;
        return chopBearing(bearing);
    }
    
    public static double getBearingVB(Coordinate c1, Coordinate c2) {

        return getBearingVB(c1.x, c1.y, c2.x, c2.y);
    }


    // POINT-LINE DISTANCE
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    /**
     * Compute the distance in meters between a {@link LineSegment} and a
     * {@link Coordinate}, using TDE functionalities. The coordinate reference
     * system must be WGS84.
     * 
     * @param point
     * @param segment
     * @return double
     */
    public static double getDistanceTDE(Coordinate point, LineSegment segment) {

        Coordinate p1, p2;
        double segLeng, projLeng;
        double alfaProjOnSegment = 0;
        double alfaProjOnSegmentLeng;

        double hypotenuse;
        double xSegm1, ySegm1, xSegm2, ySegm2;
        double xFoot, yFoot;
        double dotProduct, cosAngle;

        p1 = segment.p0;
        p2 = segment.p1;
        segLeng = GeometryUtils.getDistance(p1, p2);

        xSegm1 = p1.x;
        ySegm1 = p1.y;
        xSegm2 = p2.x;
        ySegm2 = p2.y;

        dotProduct = GeometryUtils.dotProduct(xSegm1, ySegm1, point.x, point.y, xSegm1, ySegm1, xSegm2, ySegm2);
        if (dotProduct == 0) {// the foot is on the first point of the segment
            alfaProjOnSegment = 0;
            alfaProjOnSegmentLeng = 0;
        }
        else {
            hypotenuse = GeometryUtils.getDistanceTDE(xSegm1, ySegm1, point.x, point.y);
            cosAngle = dotProduct / (hypotenuse * segLeng / RADIANTS_OF_ONE_DEGREE / MEAN_EARTH_RADIUS / RADIANTS_OF_ONE_DEGREE / MEAN_EARTH_RADIUS);
            alfaProjOnSegmentLeng = hypotenuse * cosAngle;
            alfaProjOnSegment = alfaProjOnSegmentLeng / segLeng;
        }

        if (alfaProjOnSegment <= 0) {// the foot is on the first point of the
                                     // segment
            xFoot = xSegm1;
            yFoot = ySegm1;
            alfaProjOnSegment = 0;
        }
        else if (alfaProjOnSegment >= 1) {
            xFoot = xSegm2;
            yFoot = ySegm2;
            alfaProjOnSegment = 1;
        }
        else {
            double xFactor = getXFactor(ySegm1, ySegm2);
            xFoot = (xSegm1 * xFactor + alfaProjOnSegment * xFactor * (xSegm2 - xSegm1)) / xFactor;
            yFoot = (ySegm1 + alfaProjOnSegment * (ySegm2 - ySegm1));
        }

        projLeng = getDistance(point.x, point.y, xFoot, yFoot);

        return projLeng;
    }

    /**
     * Compute the distance in meters between a {@link LineSegment} and a
     * {@link Coordinate}, using <a
     * href="http://www.vividsolutions.com/jts/JTSHome.htm">JTS</a>
     * functionalities. The coordinate reference system must be WGS84.
     * 
     * @param point
     * @param segment
     * @return double
     */
    public static double getDistanceJTS(Coordinate point, LineSegment segment) {

        double xFactor = getXFactor(new Coordinate[] { point, segment.p0, segment.p1 });

        // adjust coordinate
        Coordinate pointAd = new Coordinate(point.x * xFactor, point.y);
        LineSegment segmentAd =
                new LineSegment(new Coordinate(segment.p0.x * xFactor, segment.p0.y), new Coordinate(segment.p1.x * xFactor, segment.p1.y));

        return segmentAd.distance(pointAd) * RADIANTS_OF_ONE_DEGREE * MEAN_EARTH_RADIUS;
    }

    /**
     * Compute the distance in meters between a {@link LineSegment} and a
     * {@link Coordinate}, using JTS functionalities. The coordinate
     * reference system must be WGS84.
     * 
     * @param point
     * @param line 
     * @return double
     */
    public static double getDistanceJTSGeometry(Point point, LineString line) {

        Point[] allPoints = new Point[line.getNumPoints() + 1];
        allPoints[0] = point;
        for (int i = 1; i < allPoints.length; i++) {
            allPoints[i] = line.getPointN(i - 1);
        }

        double xFactor = getXFactor(allPoints);

        // adjust coordinate
        Point pointAd = GEOMETRY_FACTORY.createPoint(new Coordinate(point.getX() * xFactor, point.getY()));
        Coordinate[] linePointsAd = new Coordinate[line.getNumPoints()];
        Point tmp_point = null;
        for (int i = 0; i < linePointsAd.length; i++) {
            tmp_point = line.getPointN(i);
            linePointsAd[i] = new Coordinate(tmp_point.getX() * xFactor, tmp_point.getY());
        }
        LineString lineAd = GEOMETRY_FACTORY.createLineString(linePointsAd);

        return pointAd.distance(lineAd) * RADIANTS_OF_ONE_DEGREE * MEAN_EARTH_RADIUS;
    }

    /**
     * Compute the distance in meters between a {@link LineSegment} and a
     * {@link Coordinate}. The coordinate reference system must be WGS84.
     * 
     * @param point
     * @param segment
     * @return double
     */
    public static double getDistance(Coordinate point, LineSegment segment) {

        return getDistanceJTS(point, segment);
    }

    /**
     * Compute the distance in meters between a {@link LineString} and a
     * {@link Point}. The coordinate reference system must be WGS84.
     * 
     * @param point
     * @param line
     * @return double
     */
    public static double getDistance(Point point, LineString line) {

        return getDistanceJTSGeometry(point, line);
    }

    /**
     * Compute the distance in meters between a {@link LineString} and a
     * {@link Coordinate}. The coordinate reference system must be WGS84.
     * 
     * @param coordinate
     * @param line
     * @return double
     */
    public static double getDistance(Coordinate coordinate, LineString line) {

        Point p = GEOMETRY_FACTORY.createPoint(coordinate);
        return getDistanceJTSGeometry(p, line);
    }

    public static Foot pointAlongLine(LineString line, int distanceAlong, double lineLength) {

        return pointAlongLine(line, (double) distanceAlong, lineLength);

    }

    // POINT ALONG LINE
    /**
     * Return the {@link Foot} along the specified {@link LineString} located at the
     * specified distance from the first line point.
     * 
     * @param line
     * @param distanceAlong
     * @param lineLength
     *            the length in meters of the {@link LineString}
     * @return {@link Foot}
     */
    public static Foot pointAlongLine(LineString line, double distanceAlong, double lineLength) {

        if (line == null)
            return null;
        else if (distanceAlong <= 0)
            return new Foot(line.getStartPoint().getX(), line.getStartPoint().getY(), 0, 0, 1, line);
        else if (distanceAlong >= lineLength)
            return new Foot(line.getEndPoint().getX(), line.getEndPoint().getY(), 0, 1, line.getNumPoints() - 1, line);

        Point p1, p2;
        double segLeng, coveredLeng = 0;

        Foot pointAlongLine = null;

        for (int i = 0; i < line.getNumPoints() - 1; i++) {
            p1 = line.getPointN(i);
            p2 = line.getPointN(i + 1);
            segLeng = GeometryUtils.getDistance(p1, p2);
            if (coveredLeng + segLeng < distanceAlong) {// go to next segment
                coveredLeng += segLeng;
            }
            else {// segment found
                  // leng of the part of the segment from p1 to the point
                  // defined
                  // on the line according to distanceAlong value
                double progLeng = distanceAlong - coveredLeng;
                double alfa = progLeng / segLeng;// prog defined by the distance
                // on the found segment
                double xFactor = GeometryUtils.getXFactor(p1, p2);

                double xF = xFactor * (p1.getX() + alfa * (p2.getX() - p1.getX()));
                double yF = p1.getY() + alfa * (p2.getY() - p1.getY());
                pointAlongLine = new Foot(xF / xFactor, yF, 0, distanceAlong / lineLength, i + 1, line);
                break;
            }
        }

        return pointAlongLine;

    }

    /**
     * Return the {@link Foot} along the specified {@link LineString} located at the
     * specified distance from the first line point.
     * 
     * @param line
     * @param distanceAlong
     * @return {@link Foot}
     */
    public static Foot pointAlongLine(LineString line, int distanceAlong) {

        return pointAlongLine(line, distanceAlong, getLength(line));
    }

    /**
     * Return the {@link Foot} along the specified {@link LineString} located at the
     * specified progressive from the first line point.
     * 
     * @param line
     * @param progressive
     * @return {@link Foot}
     */
    public static Foot pointAlongLine(LineString line, double progressive) {

        double length = getLength(line);
        return pointAlongLine(line, progressive * length, length);
    }

    // POINT-LINE PROJECTION
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    /**
     * Return the {@link Foot} of the specified {@link Coordinate} on the given
     * {@link LineString}, using JTS functionalities.
     * 
     * @param coordinate
     * @param line
     * @return {@link Foot}
     */
    public static Foot pointToLineProjectionJTS(Coordinate coordinate, LineString line) {

        // get the nearest segment
        if (coordinate == null || line == null)
            return null;
        int nPoints = line.getNumPoints();
        double minLeng = Double.POSITIVE_INFINITY;
        int nearestsegment = 1;
        Point p1 = null;
        Point p2 = null;
        double totalLeng = getLength(line);
        double projLeng = 0;
        LineSegment segment = null;

        for (int i = 0; i < nPoints - 1; i++) {
            p1 = line.getPointN(i);
            p2 = line.getPointN(i + 1);
            segment = new LineSegment(p1.getX(), p1.getY(), p2.getX(), p2.getY());
            projLeng = GeometryUtils.getDistance(coordinate, segment);
            if (projLeng < minLeng) {
                minLeng = projLeng;
                nearestsegment = i + 1;
            }
        }

        // compute the projection
        p1 = line.getPointN(nearestsegment - 1);
        p2 = line.getPointN(nearestsegment);
        double xFactor = getXFactor(coordinate.y, p1.getY(), p2.getY());
        segment = new LineSegment(new Coordinate(p1.getX() * xFactor, p1.getY()), new Coordinate(p2.getX() * xFactor, p2.getY()));
        Coordinate coorAd = new Coordinate(coordinate.x * xFactor, coordinate.y);
        Coordinate coorFoot = null;

        double alfaProjOnSegment = segment.projectionFactor(coorAd);
        if (alfaProjOnSegment <= 0) {
            alfaProjOnSegment = 0;
            coorFoot = new Coordinate(p1.getX(), p1.getY());
        }
        else if (alfaProjOnSegment >= 1) {
            alfaProjOnSegment = 1;
            coorFoot = new Coordinate(p2.getX(), p2.getY());
        }
        else {
            coorFoot = segment.project(coorAd);
            coorFoot.x = coorFoot.x / xFactor;
        }

        double distanceFromFirstPoint = 0;
        for (int i = 0; i < nearestsegment - 1; i++) {
            p1 = line.getPointN(i);
            p2 = line.getPointN(i + 1);
            distanceFromFirstPoint += GeometryUtils.getDistance(p1, p2);
        }
        p1 = line.getPointN(nearestsegment - 1);
        p2 = line.getPointN(nearestsegment);
        double segLeng = GeometryUtils.getDistance(p1, p2);
        distanceFromFirstPoint += alfaProjOnSegment * segLeng;

        double alfaProj = totalLeng > 0 ? distanceFromFirstPoint / totalLeng : 0;

        return new Foot(coorFoot.x, coorFoot.y, minLeng, alfaProj, nearestsegment, line);

    }

    /**
     * Return the {@link Foot} of the specified {@link Coordinate} on the given
     * {@link LineString}, using TDE functionalities.
     * 
     * @param coordinate
     * @param line
     * @return {@link Foot}
     */
    public static Foot pointToLineProjectionTDE(Coordinate coordinate, LineString line) {

        Point p1, p2;
        double segLeng, projLeng;
        double minLeng = Double.POSITIVE_INFINITY;
        double minXFoot = 0;
        double minYFoot = 0;
        int nearSeg = -1;// starting from 1
        double alfaProj;
        double alfaProjOnSegment = 0;
        double alfaProjOnSegmentLeng;

        double hypotenuse;
        double xSegm1, ySegm1, xSegm2, ySegm2;
        double xFoot, yFoot;
        double dotProduct, cosAngle;
        int lineNumPoints = line.getNumPoints();
        double totalLineLength = 0;
        double progressiveLineLength = 0;
        for (int i = 0; i < lineNumPoints - 1; i++) {
            p1 = line.getPointN(i);
            p2 = line.getPointN(i + 1);
            segLeng = GeometryUtils.getDistance(p1, p2);

            xSegm1 = p1.getX();
            ySegm1 = p1.getY();
            xSegm2 = p2.getX();
            ySegm2 = p2.getY();

            if (segLeng == 0) {// the two points of the segment coincide
                xFoot = xSegm1;
                yFoot = ySegm1;
                alfaProjOnSegmentLeng = 0;
            }
            else {
                dotProduct = GeometryUtils.dotProduct(xSegm1, ySegm1, coordinate.x, coordinate.y, xSegm1, ySegm1, xSegm2, ySegm2);
                if (dotProduct == 0) {// the foot is on the first point of the
                    // segment
                    alfaProjOnSegment = 0;
                    alfaProjOnSegmentLeng = 0;
                }
                else {
                    hypotenuse = GeometryUtils.getDistance(xSegm1, ySegm1, coordinate.x, coordinate.y);
                    cosAngle = dotProduct / (hypotenuse * segLeng / RADIANTS_OF_ONE_DEGREE / MEAN_EARTH_RADIUS / RADIANTS_OF_ONE_DEGREE / MEAN_EARTH_RADIUS);
                    alfaProjOnSegmentLeng = hypotenuse * cosAngle;
                    alfaProjOnSegment = alfaProjOnSegmentLeng / segLeng;
                }

                if (alfaProjOnSegment <= 0) {// the foot is on the first point
                    // of the segment
                    xFoot = xSegm1;
                    yFoot = ySegm1;
                    alfaProjOnSegment = 0;
                }
                else if (alfaProjOnSegment >= 1) {
                    xFoot = xSegm2;
                    yFoot = ySegm2;
                    alfaProjOnSegment = 1;
                }
                else {
                    double xFactor = getXFactor(ySegm1, ySegm2);
                    xFoot = (xSegm1 * xFactor + alfaProjOnSegment * xFactor * (xSegm2 - xSegm1)) / xFactor;
                    yFoot = (ySegm1 + alfaProjOnSegment * (ySegm2 - ySegm1));
                }
            }

            projLeng = getDistance(coordinate.x, coordinate.y, xFoot, yFoot);

            if (projLeng < minLeng) {
                minLeng = projLeng;
                minXFoot = xFoot;
                minYFoot = yFoot;
                progressiveLineLength = totalLineLength + alfaProjOnSegmentLeng;
            }
            totalLineLength += segLeng;

        }
        alfaProj = progressiveLineLength / totalLineLength;

        return new Foot(minXFoot, minYFoot, minLeng, alfaProj, nearSeg, line);

    }

    /**
     * Return the {@link Foot} of the specified {@link Coordinate} on the given
     * {@link LineString}, using JTS functionalities.
     * 
     * @param coordinate
     * @param line
     * @return {@link Foot}
     */
    public static Foot pointToLineProjection(Coordinate coordinate, LineString line) {

        return pointToLineProjectionJTS(coordinate, line);
    }

    /**
     * Return the {@link Foot} of the specified {@link Point} on the given
     * {@link LineString}.
     * 
     * @param point
     * @param line
     * @return {@link Foot}
     */
    public static Foot pointToLineProjection(Point point, LineString line) {

        return pointToLineProjectionJTS(new Coordinate(point.getX(), point.getY()), line);
    }

    /**
     * Return the {@link Foot} on the nearest {@link LineString} to the given point
     * 
     * @param point
     * @param lines
     * @return {@link Foot}
     */
    public static Foot nearestLine(Point point, List<LineString> lines) {

        List<Foot> foots = lines.parallelStream().map(line -> pointToLineProjection(point, line)).collect(Collectors.toList());
        return foots.stream().min((f1, f2) -> (int) (f1.getDistance() - f2.getDistance())).get();
    }




    /**
     * Return a {@link LineString} result of the cutting operation between the positive and negative offsets
     * 
     * @param lineString
     * @param fromProgressive
     *            from the first point forward in meters
     * @param toProgressive
     *            from the last point backward in meters
     * @return <code>null</code> if fromProgressive >= toProgressive
     */
    public static LineString trimLineString(LineString lineString, double fromProgressive, double toProgressive) {

        if (fromProgressive >= toProgressive)
            return null;

        if (fromProgressive == 0 && toProgressive == 1)
            return lineString;

        Foot from = pointAlongLine(lineString, fromProgressive);
        Foot to = pointAlongLine(lineString, toProgressive);

        // get the points
        Coordinate[] coordinates = new Coordinate[to.getSegment() - from.getSegment() + 2];

        coordinates[0] = new Coordinate(from.getX(), from.getY());
        coordinates[coordinates.length - 1] = new Coordinate(to.getX(), to.getY());

        for (int i = 1; i < coordinates.length - 1; i++)
            coordinates[i] = lineString.getCoordinateN(from.getSegment() + i - 1);

        return GEOMETRY_FACTORY.createLineString(coordinates);
    }

    
    /**
     * It represent the projection (foot) of a point on a line.
     * 
     * @author simone.decristofaro 12/set/2014
     */
    public static class Foot {

        private double x;
        private double y;
        /** point-line distance */
        private double distance;

        private double alfaProj;
        private int segment;
        private LineString line;

        public Foot() {}

        /**
         * @param x
         * @param y
         * @param distance 
         * @param alfaProj	progressive along the line
         * @param segment	Index of the line segment (defined by the progressive along the line), €[1,numPoints-1]
         * @param line 
         */
        public Foot(double x, double y, double distance, double alfaProj, int segment, LineString line) {

            super();
            this.x = x;
            this.y = y;
            this.distance = distance;
            this.alfaProj = alfaProj;
            this.segment = segment;
            this.line = line;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {

            return "POINT(" + x + " " + y + ")";

        }

        /**
         * @return the x
         */
        public double getX() {

            return x;
        }

        public void setX(double x) {

            this.x = x;
        }

        /**
         * @return the y
         */
        public double getY() {

            return y;
        }

        public void setY(double y) {

            this.y = y;
        }

        /**
         * @return the distance from the first point of the line
         */
        public double getDistanceFromFirstPoint() {

            return alfaProj * GeometryUtils.getLength(line);
        }

        /**
         * @return the share of the line defined from the first point to the
         *         {@link Foot}
         */
        public double getAlfaProj() {

            return alfaProj;
        }

        public void setAlfaProj(double alfaProj) {

            this.alfaProj = alfaProj;
        }

        /**
         * @return the index of the segment of the shape containing the
         *         {@link Foot}, stating from 1
         */
        public int getSegment() {

            return segment;
        }

        public void setSegment(int segment) {

            this.segment = segment;

            return;
        }

        /**
         * @return the line
         */
        public LineString getLine() {

            return line;
        }

        /**
         * @param line
         *            the line to set
         */
        public void setLine(LineString line) {

            this.line = line;
        }

        public Point getPoint() {

            return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
        }

        /**
         * @return the {@link GeometryUtils.Foot#distance}
         */
        public double getDistance() {

            return distance;
        }

        /**
         * @param distance
         *            the {@link GeometryUtils.Foot#distance} to set
         */
        public void setDistance(double distance) {

            this.distance = distance;
        }

    }


    public static MultiPolygon convertPolygonToMultipolygon(Polygon polygon) {
    	
    	return new MultiPolygon(new Polygon[]{polygon}, GEOMETRY_FACTORY);
    }

    /**
     * Determine coordinate in distance.
     * <a href="http://www.movable-type.co.uk/scripts/latlong.html">http://www.movable-type.co.uk/scripts/latlong.html</a>
     * @param lon
     *            the lon
     * @param lat
     *            the lat
     * @param angle
     *            the Clockwise angle from North (bearing)
     * @param distance
     *            the distance in meters
     * @return the geo coordinates
     */
    public static Coordinate determineCoordinateInDistance(
            final double lon, final double lat, final int angle,
            final double distance)  {
        double lat1 = lat * RADIANTS_OF_ONE_DEGREE;
        double az12 = angle * RADIANTS_OF_ONE_DEGREE;
        double sinu1 = Math.sin(lat1);
        double cosu1 = Math.cos(lat1);
        double az12cos = Math.cos(az12);
        double az12sin = Math.sin(az12);
        double sina = cosu1 * az12sin;
        double ss = Math.sin(distance / SEMI_MAJOR_AXIS);
        double cs = Math.cos(distance / SEMI_MAJOR_AXIS);
        double g = sinu1 * ss - cosu1 * cs * az12cos;
        double lat2 = Math.atan(((sinu1 * cs + cosu1 * ss * az12cos) / (Math
                .sqrt(sina * sina + g * g)))) * HALF_CIRCLE / Math.PI;
        double lon2 = lon
                + Math.atan(ss * az12sin / (cosu1 * cs - sinu1 * ss * az12cos))
                * HALF_CIRCLE / Math.PI + (2 * FULL_CIRCLE_DEGREE);
        while (lat2 > QUARTER_CIRCLE) {
            lat2 = lat2 - FULL_CIRCLE_DEGREE;
        }
        while (lon2 > HALF_CIRCLE) {
            lon2 = lon2 - FULL_CIRCLE_DEGREE;
        }
        return new Coordinate(lon2, lat2);
    }

    /**
     * @param minX 
     * @param minY 
     * @param maxX 
     * @param maxY 
     * @return {@link Polygon}
     */
    public static Polygon createPolygonFromBounds(double minX, double minY, double maxX, double maxY) {
    
        Coordinate minXMinY = new Coordinate(minX, minY);
        Coordinate maxXMinY = new Coordinate(maxX, minY);
        Coordinate maxXMaxY = new Coordinate(maxX, maxY);
        Coordinate minXMaxY = new Coordinate(minX, maxY);
        Coordinate minXMinY2 = minXMinY;
    
        return GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
                minXMinY, maxXMinY, maxXMaxY, minXMaxY, minXMinY2
        });
    }

    
    /**
     * Create a new {@link Geometry} applying the specified buffer.
     * 
     * @param geometry
     * @param distance
     * @return {@link Geometry}
     * @see Geometry#buffer(double)
     */
    public static Geometry buffer(Geometry geometry, double distance) {
        return buffer(geometry, distance, 8, BufferParameters.CAP_ROUND);
    }
    
    /**
     * Create a new {@link Geometry} applying the specified buffer.
     * 
     * @param geometry
     * @param distance
     *            buffer distance in meters
     * @param quadrantSegments
     *            the number of line segments used to represent a quadrant of a circle
     * @param endCapStyle
     *            the end cap style to use
     * @return {@link Geometry}
     * @see Geometry#buffer(double, int, int)
     */
    public static Geometry buffer(Geometry geometry, double distance, int quadrantSegments, int endCapStyle) {
        
        Coordinate[] originalCoordinates = geometry.getCoordinates();
        
        double xFactor = getXFactor(originalCoordinates);
        
        Geometry geometryProjected = geometry.getFactory().createGeometry(geometry);
        // correct lon to do plan computations
        for (Coordinate coordinate : geometryProjected.getCoordinates()) {
            coordinate.x = coordinate.x * xFactor;
        }
        
        double bufferRadiants = distance / (MEAN_EARTH_RADIUS * RADIANTS_OF_ONE_DEGREE);
        
        Geometry geometryBuffered = geometryProjected.buffer(bufferRadiants, quadrantSegments, BufferParameters.CAP_SQUARE);
        // come back to original CRS
        for (Coordinate coordinate : geometryBuffered.getCoordinates()) {
            coordinate.x = coordinate.x / xFactor;
        }
        
        return geometryBuffered;
    }
    
    
}

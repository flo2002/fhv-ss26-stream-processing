package fhv.streamprocessing.pattern11.weatherregime;

import com.yahoo.labs.samoa.instances.DenseInstance;
import java.util.Arrays;
import moa.cluster.Clustering;
import moa.clusterers.clustream.Clustream;

public class OnlineWeatherRegimeClusterer {
    private static final String[] LABELS = {
        "cold_windy_wet",
        "cold_clear_calm",
        "mild_wet",
        "mild_clear_breezy",
        "hot_dry_clear",
        "volatile_windy"
    };
    private static final double[][] PROTOTYPES = {
        {-8.0, 8.0, 9.0, 16.0, 35.0, 9.0},
        {-5.0, 6.0, 2.0, 5.0, 85.0, 0.0},
        {9.0, 7.0, 4.0, 8.0, 45.0, 8.0},
        {15.0, 9.0, 5.0, 11.0, 80.0, 1.0},
        {28.0, 12.0, 3.0, 7.0, 90.0, 0.0},
        {12.0, 20.0, 12.0, 24.0, 55.0, 3.0}
    };

    private final Clustream clusterer;
    private final double[][] archetypes;

    public OnlineWeatherRegimeClusterer(int clusterCount, int horizon) {
        int boundedClusterCount = Math.max(1, Math.min(clusterCount, PROTOTYPES.length));
        this.archetypes = new double[boundedClusterCount][];
        for (int index = 0; index < boundedClusterCount; index++) {
            archetypes[index] = new WeatherRegimeFeatureVector(
                PROTOTYPES[index][0],
                PROTOTYPES[index][1],
                PROTOTYPES[index][2],
                PROTOTYPES[index][3],
                PROTOTYPES[index][4],
                PROTOTYPES[index][5]
            ).normalized();
        }
        this.clusterer = new Clustream();
        this.clusterer.maxNumKernelsOption.setValue(boundedClusterCount);
        this.clusterer.timeWindowOption.setValue(Math.max(horizon, boundedClusterCount));
        this.clusterer.resetLearning();
    }

    public synchronized WeatherRegimeAssignment assignAndLearn(WeatherRegimeFeatureVector featureVector) {
        double[] point = featureVector.normalized();
        clusterer.trainOnInstance(new DenseInstance(1.0, point));
        Clustering microClusters = clusterer.getMicroClusteringResult();
        if (microClusters == null || microClusters.size() == 0) {
            int nearestArchetype = nearestArchetype(point);
            return new WeatherRegimeAssignment(nearestArchetype, LABELS[nearestArchetype], round3(euclideanDistance(point, archetypes[nearestArchetype])));
        }

        int nearestCluster = nearestMicroCluster(point, microClusters);
        double[] center = microClusters.get(nearestCluster).getCenter();
        int nearestArchetype = nearestArchetype(center);
        return new WeatherRegimeAssignment(nearestCluster, LABELS[nearestArchetype], round3(euclideanDistance(point, center)));
    }

    public double[] centroid(int clusterId) {
        Clustering microClusters = clusterer.getMicroClusteringResult();
        if (microClusters != null && clusterId >= 0 && clusterId < microClusters.size()) {
            return Arrays.copyOf(microClusters.get(clusterId).getCenter(), microClusters.get(clusterId).getCenter().length);
        }
        return Arrays.copyOf(archetypes[clusterId], archetypes[clusterId].length);
    }

    private int nearestArchetype(double[] point) {
        int nearest = 0;
        double nearestDistance = Double.POSITIVE_INFINITY;
        for (int clusterId = 0; clusterId < archetypes.length; clusterId++) {
            double distance = euclideanDistance(point, archetypes[clusterId]);
            if (distance < nearestDistance) {
                nearest = clusterId;
                nearestDistance = distance;
            }
        }
        return nearest;
    }

    private int nearestMicroCluster(double[] point, Clustering microClusters) {
        int nearest = 0;
        double nearestDistance = Double.POSITIVE_INFINITY;
        for (int clusterId = 0; clusterId < microClusters.size(); clusterId++) {
            double distance = euclideanDistance(point, microClusters.get(clusterId).getCenter());
            if (distance < nearestDistance) {
                nearest = clusterId;
                nearestDistance = distance;
            }
        }
        return nearest;
    }

    private static double euclideanDistance(double[] left, double[] right) {
        double sum = 0.0;
        for (int index = 0; index < left.length; index++) {
            double delta = left[index] - right[index];
            sum += delta * delta;
        }
        return Math.sqrt(sum);
    }

    private static double round3(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }
}

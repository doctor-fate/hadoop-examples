package ru.bmstu.hadoop.spark.hero;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import java.util.TreeMap;

public class Main {
    private static final Tuple2<Float, Integer> ZERO = new Tuple2<>(0.0f, 0);

    private static Tuple2<Float, Integer> addFloatToTuple2(Tuple2<Float, Integer> a, Float v) {
        return new Tuple2<>(a._1 + v, a._2 + 1);
    }

    private static Tuple2<Float, Integer> mergeTuples2(Tuple2<Float, Integer> a, Tuple2<Float, Integer> b) {
        return new Tuple2<>(a._1 + b._1, a._2 + b._2);
    }

    public static void main(String[] args) {
        SparkConf configuration = new SparkConf();
        try (JavaSparkContext context = new JavaSparkContext(configuration)) {
            JavaRDD<Flight> flights = context.textFile("FLIGHTS.csv").
                    map(Flight::read).filter(f -> f.getCode() > 0);

            JavaRDD<Airport> airports = context.textFile("AIRPORTS.csv")
                    .map(Airport::read).filter(v -> v.getCode() > 0);

            Broadcast<Map<Integer, String>> broadcast = context.broadcast(
                    airports.mapToPair(v -> new Tuple2<>(v.getCode(), v.getName())).
                            collectAsMap());

            JavaPairRDD<Character, Float> delayed = flights.mapToPair(v -> {
                String name = broadcast.value().get(v.getCode());
                float delay = v.getDelay();
                return new Tuple2<>(Character.toUpperCase(name.charAt(0)), delay);
            }).filter(v -> Float.compare(v._2, 0.0f) > 0);

            Map<Character, Tuple2<Float, Integer>> aggregated = delayed.aggregateByKey(
                    ZERO, Main::addFloatToTuple2, Main::mergeTuples2).collectAsMap();

            Map<Character, Tuple2<Float, Integer>> result = new TreeMap<Character, Tuple2<Float, Integer>>() {
                {
                    for (char c = 'A'; c <= 'Z'; c++) {
                        put(c, aggregated.getOrDefault(c, ZERO));
                    }
                }
            };

            result.forEach((key, value) -> System.out.println(key + " --> (" + value._1 + ";" + value._2 + ")"));
        }
    }
}
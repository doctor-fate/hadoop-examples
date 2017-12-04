package ru.bmstu.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import ru.bmstu.hadoop.validators.Validator;
import scala.Tuple2;

import java.util.Map;
import java.util.Optional;

public class Main {
    private static final int AIRPORT_ID_CSV_IDX = 0;
    private static final int AIRPORT_NAME_CSV_IDX = 0;

    private static Tuple2<Integer, String> parseAirport(String input) {
        String[] splitted = input.replaceAll("\"", "").split(",", 2);
        String name = splitted[AIRPORT_NAME_CSV_IDX];
        Optional<Integer> id = Validator.validateInteger(splitted[AIRPORT_ID_CSV_IDX]);
        return new Tuple2<>(id.orElse(0), name);
    }

    private static Tuple2<OriginDestination, Flight> parseFlight(String input) {
        OriginDestination e = OriginDestination.read(input);
        Flight flight = Flight.read(input);
        return new Tuple2<>(e, flight);
    }

    public static void main(String[] args) {
        SparkConf configuration = new SparkConf();
        try (JavaSparkContext context = new JavaSparkContext(configuration)) {
            JavaPairRDD<OriginDestination, Flight> flights = context.textFile("FLIGHTS.csv")
                    .mapToPair(Main::parseFlight)
                    .filter(e -> e._1.isValid());

            JavaPairRDD<OriginDestination, Statistics> statistics =
                    flights.aggregateByKey(Statistics.ZERO, Statistics::add, Statistics::merge);

            JavaPairRDD<Integer, String> airports = context.textFile("AIRPORTS.csv")
                    .mapToPair(Main::parseAirport)
                    .filter(e -> e._1 != 0);

            Broadcast<Map<Integer, String>> broadcast = context.broadcast(airports.collectAsMap());
            statistics.map(e -> {
                String origin = broadcast.value().get(e._1.getOrigin());
                String destination = broadcast.value().get(e._1.getDestination());
                return new Tuple2<>(new Tuple2<>(origin, destination), e._2);
            }).collect().forEach(System.out::println);
        }
    }
}

// 2. Часы по дням недели больше всего опоздавших.
// 3. Кол-во опозданий в обе стороны. Сумма. Сортировка
// 4. Для каждой буквы алфавита опоздания прилет.
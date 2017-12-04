FLIGHTS = LOAD 'FLIGHTS.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

FLIGHTS = FOREACH FLIGHTS GENERATE
    (int) $11, (int) $14,
    (float) $18 AS delay;

FLIGHTS = FOREACH FLIGHTS GENERATE
    ($0 < $1 ? ($0, $1) : ($1, $0)) AS route,
    (delay > 0 ? 1 : 0) AS is_delayed;

FLIGHTS = FILTER FLIGHTS BY route != (0, 0);

GROUPED_FLIGHTS_BY_ROUTE = GROUP FLIGHTS BY route;

AGGREGATED_FLIGHTS = FOREACH GROUPED_FLIGHTS_BY_ROUTE GENERATE
    group AS route,
    SUM(FLIGHTS.is_delayed) AS num_delayed;

/* Join part. Not neccesary */
AIRPORTS = LOAD 'AIRPORTS.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (code:int, name:chararray);
AIRPORTS = FOREACH AIRPORTS GENERATE code, CONCAT(CONCAT('"', name), '"') AS name;

AIRPORTS_FLIGHTS = JOIN AIRPORTS BY code, AGGREGATED_FLIGHTS by route.$0;

AIRPORTS_FLIGHTS = FOREACH AIRPORTS_FLIGHTS GENERATE
    (AIRPORTS::name, AGGREGATED_FLIGHTS::route.$1) AS route,
    AGGREGATED_FLIGHTS::num_delayed AS num_delayed;

AIRPORTS_FLIGHTS = JOIN AIRPORTS BY code, AIRPORTS_FLIGHTS by route.$1;

AIRPORTS_FLIGHTS = FOREACH AIRPORTS_FLIGHTS GENERATE
        (AIRPORTS_FLIGHTS::route.$0, AIRPORTS::name) AS route,
        AIRPORTS_FLIGHTS::num_delayed as num_delayed;

SORTED_AIRPORTS_FLIGHTS = ORDER AIRPORTS_FLIGHTS BY num_delayed ASC;

dump SORTED_AIRPORTS_FLIGHTS;
package part2;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import shopping.CatalogEvent;
import shopping.CatalogEventsGenerator;
import shopping.ShoppingCartEvent;
import shopping.SingleShoppingCartEventsGenerator;

import java.time.Duration;
import java.util.Optional;

public class MultipleStreams {

    /*
        - union
        - window joins
        - interval joins
        - connect
     */

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Unioning = combining the output of multiple streams into just one
    public static void demoUnioning() throws Exception {

        // define two streams of the same type
        DataStream<ShoppingCartEvent> shoppingCartEventsKafka = env.addSource(new SingleShoppingCartEventsGenerator(
                300, java.time.Instant.now(), Optional.of(0L), Optional.of("kafka"), false
        ));

        DataStream<ShoppingCartEvent> shoppingCartEventsFiles = env.addSource(new SingleShoppingCartEventsGenerator(
                1000, java.time.Instant.now(), Optional.of(0L), Optional.of("files"), false
        ));

        DataStream<ShoppingCartEvent> combinedShoppingCartEventsStream = shoppingCartEventsKafka
                .union(shoppingCartEventsFiles);

        combinedShoppingCartEventsStream.print();
        env.execute();

    }

     // window join = elements belong to the same window + same join condition
    public static void demoWindowJoins() throws Exception {

        // shopping cart events
        DataStream<ShoppingCartEvent> shoppingCartEventsKafka = env.addSource(new SingleShoppingCartEventsGenerator(
                300, java.time.Instant.now(), Optional.of(0L), Optional.of("kafka"), false
        ));

        // catalog events
        DataStream<CatalogEvent> catalogEventsStream = env.addSource(new CatalogEventsGenerator(
                200, java.time.Instant.now(), Optional.of(0L)
        ));

        DataStream<String> joinedStream = shoppingCartEventsKafka
                .join(catalogEventsStream)
                // provide join condition
                .where(ShoppingCartEvent::getUserId)
                .equalTo(CatalogEvent::getUserId)
                // provide the same window grouping
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // do something
                .apply(new JoinFunction<ShoppingCartEvent, CatalogEvent, String>() {
                    @Override
                    public String join(ShoppingCartEvent first, CatalogEvent second) throws Exception {
                        return first.getUserId() + "browsed at " + second.getTime() + " and bought at " +
                                first.getTime();
                    }
                });

        joinedStream.print();
        env.execute();
    }

    // interval joins = correlation between events A and B if durationMin < timeA - timeB > durationMax
    // involves EVENT TIME
    // only works on KEYED STREAMS

    public static void demoIntervalJoins() throws Exception {

        // need to extract event time from both streams
        // shopping cart events
        KeyedStream<ShoppingCartEvent, String> shoppingCartEventsKafka = env.addSource(new SingleShoppingCartEventsGenerator(
                300, java.time.Instant.now(), Optional.of(0L), Optional.of("kafka"), false
        )).assignTimestampsAndWatermarks(WatermarkStrategy.<ShoppingCartEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
                .withTimestampAssigner((SerializableTimestampAssigner<ShoppingCartEvent>) (element, recordTimestamp) ->
                        element.getTime().toEpochMilli())
        )
        .keyBy((KeySelector<ShoppingCartEvent, String>) ShoppingCartEvent::getUserId);

        // catalog events
        KeyedStream<CatalogEvent, String> catalogEventsStream = env.addSource(new CatalogEventsGenerator(
                500, java.time.Instant.now(), Optional.of(0L)
        )).assignTimestampsAndWatermarks(WatermarkStrategy.<CatalogEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
                .withTimestampAssigner((SerializableTimestampAssigner<CatalogEvent>) (element, recordTimestamp) ->
                        element.getTime().toEpochMilli())
        ).keyBy((KeySelector<CatalogEvent, String>) CatalogEvent::getUserId);

        DataStream<String> intervalJoinedStream = shoppingCartEventsKafka
                .intervalJoin(catalogEventsStream)
                .between(Time.seconds(-2), Time.seconds(2))
                .lowerBoundExclusive() // interval by default is inclusive
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<ShoppingCartEvent, CatalogEvent, String>() {
                    @Override
                    public void processElement(ShoppingCartEvent shoppingCartEvent, CatalogEvent catalogEvent, ProcessJoinFunction<ShoppingCartEvent, CatalogEvent, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect("User " + shoppingCartEvent.getUserId() + "browsed at " + catalogEvent.getTime() +
                                " and bought at " + shoppingCartEvent.getTime());
                    }
                });

        intervalJoinedStream.print();
        env.execute();
    }

    // connect = two streams are treated with the same operator
    public static void demoConnect() throws Exception {

        // shopping cart events
        DataStream<ShoppingCartEvent> shoppingCartEventsKafka = env.addSource(new SingleShoppingCartEventsGenerator(
                100, java.time.Instant.now(), Optional.of(0L), Optional.of("kafka"), false
        )).setParallelism(1);

        // catalog events
        DataStream<CatalogEvent> catalogEventsStream = env.addSource(new CatalogEventsGenerator(
                1000, java.time.Instant.now(), Optional.of(0L)
        )).setParallelism(1);

        // connect the streams
        ConnectedStreams<ShoppingCartEvent, CatalogEvent> connectStream = shoppingCartEventsKafka
                .connect(catalogEventsStream);

        // variables will use single-threaded
        env.setParallelism(1);
        env.setMaxParallelism(1);

        // variables - will use single-threaded
        DataStream<Double> ratioStream = connectStream.process(new CoProcessFunction<ShoppingCartEvent, CatalogEvent, Double>() {
            Integer shoppingCartEventCount = 0;
            Integer catalogEventCount = 0;

            @Override
            public void processElement1(ShoppingCartEvent shoppingCartEvent, CoProcessFunction<ShoppingCartEvent,
                    CatalogEvent, Double>.Context context, Collector<Double> collector) throws Exception {
                shoppingCartEventCount += 1;
                collector.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount));
            }

            @Override
            public void processElement2(CatalogEvent catalogEvent, CoProcessFunction<ShoppingCartEvent,
                    CatalogEvent, Double>.Context context, Collector<Double> collector) throws Exception {
                catalogEventCount += 1;
                collector.collect(catalogEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount));
            }
        });

        ratioStream.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoConnect();
    }

}

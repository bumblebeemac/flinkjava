package part3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import shopping.AddToShoppingCartEvent;
import shopping.ShoppingCartEvent;
import shopping.ShoppingCartEventsGenerator;

import java.time.Instant;

public class BroadcastState {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    static DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
            100, 5, Instant.parse("2024-02-15T00:00:00.000Z")));
    static KeyedStream<ShoppingCartEvent, String> eventsByUser = shoppingCartEvents.keyBy(ShoppingCartEvent::getUserId);

    // issue a warning if quantity > threshold
    public static void purchaseWarnings(Long threshold) throws Exception {

        DataStream<String> notificationsStream = eventsByUser
                .filter(event -> event instanceof AddToShoppingCartEvent)
                .filter(new FilterFunction<>() {
                    @Override
                    public boolean filter(ShoppingCartEvent value) throws Exception {
                        AddToShoppingCartEvent atcEvent = (AddToShoppingCartEvent) value;
                        return atcEvent.getQuantity() > threshold;
                    }
                })
                .map(new MapFunction<ShoppingCartEvent, String>() {
                    @Override
                    public String map(ShoppingCartEvent value) throws Exception {
                        AddToShoppingCartEvent atcEvent = (AddToShoppingCartEvent) value;
                        return "User " + atcEvent.getUserId() + " attempting to purchase " + atcEvent.getQuantity() +
                                " items of " + atcEvent.getSku() + "when threshold is " + threshold;
                    }
                });

        notificationsStream.print();
        env.execute();
    }

    // ... if the threshold CHANGES over time?
    // thresholds will be BROADCAST

    public static void changingThresholds() throws Exception {
        DataStream<Integer> thresholds = env.addSource(new SourceFunction<>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                Integer[] thresholdValues = new Integer[]{2,0,4,5,6,3};
                for (Integer t : thresholdValues) {
                    Thread.sleep(1000);
                    sourceContext.collect(t);
                }
            }

            @Override
            public void cancel() {

            }
        });

        // broadcast state is ALWAYS a map
        MapStateDescriptor<String, Integer> broadcastStateDescriptor = new MapStateDescriptor<>("thresholds",
                String.class, Integer.class);
        BroadcastStream<Integer> broadcastThresholds = thresholds.broadcast(broadcastStateDescriptor);

        DataStream<String> notificationStream = eventsByUser
                .connect(broadcastThresholds)
                .process(new KeyedBroadcastProcessFunction<String, ShoppingCartEvent, Integer,   String>() {
                    //                                     ^key    ^first event       ^broadcast ^ Output

                    final MapStateDescriptor<String, Integer> thresholdDescriptor = new MapStateDescriptor<>("thresholds",
                            String.class, Integer.class);

                    @Override
                    public void processElement(ShoppingCartEvent shoppingCartEvent, KeyedBroadcastProcessFunction<String,
                            ShoppingCartEvent, Integer, String>.ReadOnlyContext readOnlyContext, Collector<String> collector)
                            throws Exception {

                        Integer currentThreshold = readOnlyContext.getBroadcastState(thresholdDescriptor)
                                .get("quantity-threshold");

                        if (shoppingCartEvent instanceof AddToShoppingCartEvent) {
                            AddToShoppingCartEvent atcEvent = (AddToShoppingCartEvent) shoppingCartEvent;
                            if (atcEvent.getQuantity() > currentThreshold) {
                                collector.collect("User " + atcEvent.getUserId() + " attempting to purchase " +
                                        atcEvent.getQuantity() + " items of " + atcEvent.getSku() +
                                        "when threshold is " + currentThreshold);
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(Integer integer, KeyedBroadcastProcessFunction<String,
                            ShoppingCartEvent, Integer, String>.Context context, Collector<String> collector)
                            throws Exception {
                        System.out.println("Threshold about to be changed: -- " + integer);
                        // fetch the broadcast state
                        org.apache.flink.api.common.state.BroadcastState<String, Integer> stateThresholds =
                                context.getBroadcastState(thresholdDescriptor);
                        // update the state
                        stateThresholds.put("quantity-threshold", integer);
                    }
                });

        notificationStream.print();
        env.execute();
    }


    public static void main(String[] args) throws Exception {
        changingThresholds();
    }
}

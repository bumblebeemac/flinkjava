package part3;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import shopping.ShoppingCartEvent;
import shopping.ShoppingCartEventsGenerator;

import java.time.Instant;

public class KeyedState {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    static DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
            100, 5, Instant.parse("2024-02-15T00:00:00.000Z")));

    static KeyedStream<ShoppingCartEvent, String> eventsPerUser = shoppingCartEvents.keyBy(ShoppingCartEvent::getUserId);

    public static void demoValueState() throws Exception {

        /*
            How many events PER USER have been generated?
         */

        DataStream<String> numEventsPerUserNaive = eventsPerUser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
                                                                                          // ^ Key   ^ event            ^ result
            Integer nEventsForThisUser = 0;

            @Override
            public void processElement(ShoppingCartEvent value, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                nEventsForThisUser += 1;
                out.collect("User " + value.getUserId() + "- " + nEventsForThisUser);
            }
        });

        /*
            Problems with local variables
            - they are local, other nodes don't see them
            - if a node crashes, the variable disappears (State will be COMPLETELY lost)
         */

        DataStream<String> numEventsPerUserStream = eventsPerUser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

            // call .value to get current state
            // can also call .update(newValue) to overwrite current value
            private transient ValueState<Long> stateCounter; // a value state per key=userId

            @Override
            public void open(Configuration parameters) throws Exception {
                // initialize all state
                stateCounter = getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("events-counter", Long.class));
            }

            @Override
            public void processElement(ShoppingCartEvent value, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                // Obtain current value stored in ValueState
                Long nEventsForThisUser = stateCounter.value();
                stateCounter.update(nEventsForThisUser + 1);
                out.collect("User " + value.getUserId() + "- " + (nEventsForThisUser + 1));
            }

        });

    }

    // ListState
    public static void demoListState() throws Exception {
        // store all events per userId

        DataStream<String> allEventsPerUserStream = eventsPerUser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

            // create state here
            /*
                Capabilities
                - add(value)
                - addAll(list)
                - update(new list) - overwriting
                - get
             */
            private transient ListState<ShoppingCartEvent> stateEventsPerUser; // once per key
            // you need to be careful to keep the size of the list BOUNDED

            // initialization
            @Override
            public void open(Configuration parameters) throws Exception {
                stateEventsPerUser = getRuntimeContext()
                        .getListState(new ListStateDescriptor<ShoppingCartEvent>("shopping-cart-events",
                                ShoppingCartEvent.class));
            }

            @Override
            public void processElement(ShoppingCartEvent shoppingCartEvent, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context context, Collector<String> collector) throws Exception {
                stateEventsPerUser.add(shoppingCartEvent);
                Iterable<ShoppingCartEvent> currentEvents = stateEventsPerUser.get(); // does not return a plain List, Java Iterable

                collector.collect("User " + shoppingCartEvent.getUserId() + " - " + "[" +
                        StringUtils.join(currentEvents, ",") + "]");
            }
        });

        allEventsPerUserStream.print();
        env.execute();
    }

    // MapState
    public static void demoMapState() throws Exception {
        // count how events PER TYPE were ingested PER USER

        DataStream<String> streamOfCountPerType = eventsPerUser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

            // create state
            private transient MapState<String, Long> stateCountsPerEventType;

            // initialize state here
            @Override
            public void open(Configuration parameters) throws Exception {
                stateCountsPerEventType = getRuntimeContext()
                        .getMapState(new MapStateDescriptor<>("per-type-counter", String.class, Long.class));
            }

            @Override
            public void processElement(ShoppingCartEvent shoppingCartEvent, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context context, Collector<String> collector) throws Exception {
                // fetch the type of event
                String eventType = shoppingCartEvent.getClass().getSimpleName();
                if (stateCountsPerEventType.contains(eventType)) {
                    Long oldCount = stateCountsPerEventType.get(eventType);
                    Long newCount = oldCount + 1;
                    stateCountsPerEventType.put(eventType, newCount);
                } else {
                    stateCountsPerEventType.put(eventType, 1L);
                }

                collector.collect(context.getCurrentKey() + " - " +
                        StringUtils.join(stateCountsPerEventType.entries(), ","));
            }

        });

        streamOfCountPerType.print();
        env.execute();
    }

    // clear the state manually
    // clear the state at a regular interval
    public static void demoListStateWithClearance() throws Exception {

        DataStream<String> allEventsPerUserStream = eventsPerUser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

            // If more than 10 elements, clear the list
            private transient ListState<ShoppingCartEvent> stateEventsPerUser;
            // initialize state
            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<ShoppingCartEvent> descriptor = new ListStateDescriptor<ShoppingCartEvent>(
                        "shopping-cart-events", ShoppingCartEvent.class);
                // time to live = cleared if it's not modified after a certain time
                descriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.hours(1)) // clears the state after 1 hour
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // specify when timer resets
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // return old value between request and when cleanup actually happens
                                .build()
                );

                stateEventsPerUser = getRuntimeContext()
                        .getListState(descriptor);

            }

            @Override
            public void processElement(ShoppingCartEvent shoppingCartEvent, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context context, Collector<String> collector) throws Exception {
                stateEventsPerUser.add(shoppingCartEvent);
                Iterable<ShoppingCartEvent> currentEvents = stateEventsPerUser.get();
                if (Iterables.size(currentEvents) > 10) {
                    stateEventsPerUser.clear(); // clearing is not done immediately
                }

                collector.collect("User " + shoppingCartEvent.getUserId() + " - " + "[" +
                        StringUtils.join(currentEvents, ",") + "]");
            }
        });

        allEventsPerUserStream.print();
        env.execute();
    }



    public static void main(String[] args) throws Exception {
        demoListStateWithClearance();
    }
}

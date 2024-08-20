package part2;

import org.apache.flink.api.common.state.ValueState;
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

    public static void demoFirstKeyedState() throws Exception {
        DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
                100, 5, Instant.parse("2024-02-15T00:00:00.000Z")));

        /*
            How many events PER USER have been generated?
         */
        KeyedStream<ShoppingCartEvent, String> eventsPeruser = shoppingCartEvents.keyBy(ShoppingCartEvent::getUserId);

        DataStream<String> numEventsPerUserNaive = eventsPeruser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
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

        DataStream<String> numEventsPerUserStream = eventsPeruser.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

            private transient ValueState<Long> stateCounter;

            @Override
            public void processElement(ShoppingCartEvent value, KeyedProcessFunction<String,
                    ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {

            }

        });

    }


    public static void main(String[] args) {

    }
}

package part3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import shopping.AddToShoppingCartEvent;
import shopping.SingleShoppingCartEventsGenerator;

import java.time.Instant;
import java.util.Optional;

public class RichFunctions {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    static DataStream<Integer> numberStream = env.fromElements(1,2,3,4,5);

    static DataStream<Integer> tenXNumbers = numberStream.map((element) -> element * 10);

    // Rich map function
    static DataStream<Integer> tenXNumbers_V2 = numberStream.map(new RichMapFunction<Integer, Integer>() {
        @Override
        public Integer map(Integer value) throws Exception {
            return value * 10;
        }
    });

    // Rich map function + life cycle methods
    static DataStream<Integer> tenXNumbers_V3 = numberStream.map(new RichMapFunction<>() {
        @Override
        public Integer map(Integer value) throws Exception { // mandatory override
            return value * 10;
        }

        // REFER DOCUMENTATION
    });

    // ProcessFunction - the most general function abstraction in Flink
    static DataStream<Integer> tenXNumbersProcess = numberStream.process(new ProcessFunction<Integer, Integer>() {
        @Override
        public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out)
                throws Exception {
            out.collect(value * 10);
        }

        // also override lifecycle methods
    });

    // Exercise
    static StreamExecutionEnvironment exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    static DataStream<AddToShoppingCartEvent> shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(
            100, Instant.now(), Optional.of(0L), Optional.of("kafka"), false
    ))
            .filter(event -> event instanceof AddToShoppingCartEvent)
            .map(event -> (AddToShoppingCartEvent) event);

    // 1 - lambdas
    static DataStream<String> itemsPurchasedStream = shoppingCartEvents.flatMap(new FlatMapFunction<>() {
        @Override
        public void flatMap(AddToShoppingCartEvent value, Collector<String> out) throws Exception {
            for (int i = 1; i < value.getQuantity(); i++) {
                out.collect(value.getSku());
            }
        }
    });

    // Rich FlatMap Function same as above but lifecycle methods can be implemented

    static DataStream<String> itemsPurchasedStreamV2 = shoppingCartEvents.process(new ProcessFunction<AddToShoppingCartEvent, String>() {
        @Override
        public void processElement(AddToShoppingCartEvent value, ProcessFunction<AddToShoppingCartEvent, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            for (int i = 1; i < value.getQuantity(); i++) {
                out.collect(value.getSku());
            }
        }
    });

    public static void main(String[] args) throws Exception {
        itemsPurchasedStreamV2.print();
        exerciseEnv.execute();
    }
}

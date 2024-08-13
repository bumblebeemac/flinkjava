package part2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import shopping.ShoppingCartEvent;
import shopping.SingleShoppingCartEventsGenerator;

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

    }


    public static void main(String[] args) throws Exception {

    }

}

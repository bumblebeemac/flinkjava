package part2;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import shopping.ShoppingCartEvent;
import shopping.SingleShoppingCartEventsGenerator;

import java.util.Optional;

public class Partitions {

    // splitting = partitions

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void demoPartitioner() throws Exception {
        DataStream<ShoppingCartEvent> shoppingcartEvents = env.addSource(new SingleShoppingCartEventsGenerator(
                100, java.time.Instant.now(), Optional.of(0L), Optional.of("kafka"), false));

        // partitioner = logic to split data
        Partitioner<String> partitioner = (key, numPartitions) -> { // invoked on every event
            // hash code % number of partitions ~ even distribution
            System.out.println("Number of max partitions" + numPartitions);
            return key.hashCode() & numPartitions;
        };

        /*
            Bad because
            - you lose parallelism
            - you risk overloading task with disproportionate data

            Good for e.g: sending HTTP requests
         */
        Partitioner<String> badPartitioner = (key, numPartitions) -> numPartitions - 1; // last partition index

        DataStream<ShoppingCartEvent> partitionedStream = shoppingcartEvents.partitionCustom(
                badPartitioner, ShoppingCartEvent::getUserId)
                .shuffle() // redistribute data even if you use a badPartitioner but random (through network-costly)
                ;

        partitionedStream.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoPartitioner();
    }

}

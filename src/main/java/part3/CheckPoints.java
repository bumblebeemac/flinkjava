package part3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import shopping.AddToShoppingCartEvent;
import shopping.ShoppingCartEvent;
import shopping.ShoppingCartEventsGenerator;

import java.time.Instant;

public class CheckPoints {

    public static void demoCheckpoint() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set checkpoint intervals
        env.getCheckpointConfig().setCheckpointInterval(100);
        // set checkpoint storage
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/r0s0fin/Documents/GitHubRepos/flinkjava/checkpoints");

        /*
            Keep track of AddToCart events PER USER, when quantity > a threshold (e.g. managing stock)
            Persist the data (state) via checkpoints
         */

        DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(new ShoppingCartEventsGenerator(
                100, 5, Instant.parse("2024-02-15T00:00:00.000Z")));

        DataStream<Tuple2<String, Long>> eventsByUser = shoppingCartEvents.keyBy(ShoppingCartEvent::getUserId)
                .flatMap(new HighQuantityCheckpointFunction(5L));

        eventsByUser.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        demoCheckpoint();
    }

    public static class HighQuantityCheckpointFunction implements FlatMapFunction<ShoppingCartEvent, Tuple2<String, Long>>,
            CheckpointedFunction, CheckpointListener {
        // instance variable(s)
        Long threshold;

        // Constructor
        public HighQuantityCheckpointFunction(Long threshold) {
            this.threshold = threshold;
        }

        private transient ValueState<Long> stateCount; // instantiated per KEY

        @Override
        public void flatMap(ShoppingCartEvent value, Collector<Tuple2<String, Long>> out) throws Exception {
            if (value instanceof AddToShoppingCartEvent) {
                AddToShoppingCartEvent event = (AddToShoppingCartEvent) value;
                if (event.getQuantity() > threshold) {
                    // update state
                    Long newUserEventCount = stateCount.value() + 1L;
                    stateCount.update(1L);

                    // push output
                    out.collect(new Tuple2<>(event.getUserId(), newUserEventCount));
                }
            }
        }

        // invoked when checkpoint is triggered
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("Checkpoint at " + functionSnapshotContext.getCheckpointTimestamp());
        }

        // lifecycle method to initialize state (~ open in RichFunction)
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ValueStateDescriptor<Long> stateCountDescriptor = new ValueStateDescriptor<>("impossibleOrderCount",
                    Long.class);
            stateCount = functionInitializationContext.getKeyedStateStore().getState(stateCountDescriptor);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {

        }

        // Not mandatory
        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {

        }
    }
}

package playground;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Playground {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> simpleNumbers = env.fromElements(1,2,3,4,5);
        simpleNumbers.print();
        env.execute();
    }

}

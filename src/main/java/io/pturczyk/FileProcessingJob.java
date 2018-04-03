package io.pturczyk;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class FileProcessingJob {

    private static class WordLengthMapper implements FlatMapFunction<String, Tuple1<Integer>> {
        @Override
        public void flatMap(String input, Collector<Tuple1<Integer>> out) {
            Arrays.stream(input.split(" |,"))
                    .map(String::trim)
                    .filter(StringUtils::isNoneEmpty)
                    .forEach(s -> out.collect(Tuple1.of(s.length())));
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile("src/main/resources/shakespeare.txt")
                .map(String::trim)
                .flatMap(new WordLengthMapper())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(50)))
                .reduce((ReduceFunction<Tuple1<Integer>>) (integerTuple1, t1) -> Tuple1.of(integerTuple1.f0 + t1.f0))
                .print();

        env.execute();
    }
}

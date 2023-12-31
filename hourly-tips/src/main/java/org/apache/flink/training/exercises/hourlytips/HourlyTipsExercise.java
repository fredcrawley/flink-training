/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source);

        // replace this with your solution
//        if (true) {
//            throw new MissingSolutionException();
//        }

        // the results should be sent to the sink that was passed in
        // (otherwise the tests won't work)
        // you can end the pipeline with something like this:

        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
        // hourlyMax.addSink(sink);


        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> result = fares.assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps().withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()))

//                .map(new MapFunction<TaxiFare, Tuple2<Long,Float>>() {
//            @Override
//            public Tuple2<Long, Float> map(TaxiFare value) throws Exception {
//                return new Tuple2<Long, Float>.(value.driverId, value.tip + value.totalFare);
//            }
//        });
//                .map(t -> new Tuple2<Long, Float>(t.driverId, t.tip + t.totalFare))
                .keyBy(t -> t.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
//                .reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1));
//                .reduce((v1, v2) -> Tuple2.of(v1.driverId, v1.totalFare + v1.tip + v2.totalFare + v2.tip));
                .aggregate(new AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>>() {

                    Tuple2<Long, Float> acc = new Tuple2<Long, Float>(0L, 0F);

                    @Override
                    public Tuple2<Long, Float> createAccumulator() {
                        return acc;
                    }

                    @Override
                    public Tuple2<Long, Float> add(TaxiFare value, Tuple2<Long, Float> accumulator) {
                        return Tuple2.of(accumulator.f0, accumulator.f1 + value.tip);
                    }

                    @Override
                    public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {

                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
                        Tuple2<Long, Float> next = elements.iterator().next();
                        out.collect(Tuple3.of(context.window().getEnd(), key, next.f1));
                    }
                });

        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> result2 = result.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);


        result2.addSink(new PrintSinkFunction<>());


//                .map( t -> new Tuple2<Long, Float>(t.driverId, t.tip + t.totalFare ))
//                .reduce(new ReduceFunction<Float>() {
//            @Override
//            public Float reduce(TaxiFare value1, TaxiFare value2) throws Exception {
//                return value1.totalFare + value1.tip + value2.totalFare + value2.tip;
//            }
//        }


        // execute the pipeline and return the result
        return env.execute("Hourly Tips");
    }
}

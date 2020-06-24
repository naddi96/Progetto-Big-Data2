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

package naddi.sadb.progetto;

import naddi.sadb.progetto.query1Utils.BussDelay;
import naddi.sadb.progetto.query1Utils.utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class Query1 {



	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		//Time time = Time.hours(1);
		 Time time = Time.days(1);
		// private Time time = Time.days(7);
		//private Time time = Time.days(30);

		final String hostname="127.0.0.1";
		final int port =444;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		properties.setProperty("zookeeper.connect", "localhost:2181");


		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
				"localhost:9092,localhost:9093,localhost:9094",            // broker list
				"streams-wordcount-output",                  // target topic
				new SimpleStringSchema());   // serialization schema
		myProducer.setWriteTimestampToKafka(true);




		DataStream<String> text = env
				.addSource(new FlinkKafkaConsumer<>("streams", new SimpleStringSchema(), properties));
		//DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<Tuple2<String, BussDelay>> parsed_data=text.flatMap(new parseFlatMap());
		MapReduceFunctions.putEventTime(parsed_data, Time.hours(0)).keyBy(0)
				.window((TumblingEventTimeWindows.of(time)))
				.reduce((x, y) -> MapReduceFunctions.reduceBoro(x, y))
				.map(new computeAvgMap())
				.windowAll((TumblingEventTimeWindows.of(time)))
				.apply(new AllWindowFunction<Tuple5<String, String, String,Long,Long>, String, TimeWindow>() {
					@Override
						public void apply(TimeWindow timeWindow, Iterable<Tuple5<String, String, String,Long,Long>> iterable, Collector<String> collector) throws Exception {
							String output = "";
							String date = "7000-09-07T07:41:00.000";
							long max=Long.MIN_VALUE;
							long min=Long.MAX_VALUE;
						for (Tuple5<String, String, String,Long,Long> rec : iterable) {
							date = utils.returnMinDate(rec.f2, date);
							max = Math.max(max,rec.f3);
							min= Math.min(min,rec.f4);
							output = output + rec.f0 + "," + rec.f1 + ",";
						}
						output = max + "," + min + "," + date + "," + output;
						collector.collect(output);
					}
				})
		.addSink(myProducer);
		env.execute("Socket Window WordCount");

	}




	// ------------------------------------------------------------------------



	/**
	 * Data type for words with count.
	 */

}
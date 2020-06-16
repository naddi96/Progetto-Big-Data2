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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

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

		final String hostname="127.0.0.1";
		final int port =444;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//CPU_CORES= new ConfigOption("12");

		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// parse the data, group it, window it, and aggregate the counts

		DataStream<Tuple2<String, BussDelay>> parsed_data=text.flatMap(new parseFlatMap());

			MapReduceFunctions.putEventTime(parsed_data,Time.hours(1)).keyBy(0)
			.window((TumblingEventTimeWindows.of(Time.days(30))))
			.reduce((x,y) ->MapReduceFunctions.reduceBoro(x,y))
			.map(new computeAvgMap())
		.print().setParallelism(1);
		env.execute("Socket Window WordCount");

	}




	// ------------------------------------------------------------------------



	/**
	 * Data type for words with count.
	 */

}
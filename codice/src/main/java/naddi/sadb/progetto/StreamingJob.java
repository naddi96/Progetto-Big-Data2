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

import com.sun.xml.internal.ws.api.ha.StickyFeature;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.apache.flink.configuration.TaskManagerOptions.CPU_CORES;

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
public class StreamingJob {




	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final String hostname="127.0.0.1";
		final int port =444;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//CPU_CORES= new ConfigOption("12");

		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// parse the data, group it, window it, and aggregate the counts

		DataStream<Tuple2<String,BussDelay>> parsed_data=text.flatMap(new parseFlatMap());

		SingleOutputStreamOperator<Tuple2<String,BussDelay>> parsed_data_time =
			MapReduceFunctions.putEventTime(parsed_data,Time.hours(1));

		KeyedStream<Tuple2<String, BussDelay>, Tuple> key_by_boro =
				parsed_data_time.keyBy(0);

		WindowedStream<Tuple2<String, BussDelay>, Tuple, TimeWindow> timed_windows =
				key_by_boro.window((TumblingEventTimeWindows.of(Time.days(30))));

		SingleOutputStreamOperator<Tuple2<String, BussDelay>> reduced_by_boro =
				timed_windows.reduce((x,y) ->MapReduceFunctions.reduceBoro(x,y));

		SingleOutputStreamOperator<Tuple3<String,String,String>>
				datafinale = reduced_by_boro.map(new computeAvgMap());

		datafinale.print().setParallelism(1);
		env.execute("Socket Window WordCount");

	}




	// ------------------------------------------------------------------------



	/**
	 * Data type for words with count.
	 */

}
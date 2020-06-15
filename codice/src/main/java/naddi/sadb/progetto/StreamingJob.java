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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// parse the data, group it, window it, and aggregate the counts

		DataStream<Tuple2<String,BussDelay>> windowCoun=text.flatMap( new FlatMapFunction<String, Tuple2<String,BussDelay>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String,BussDelay>> out)
					throws Exception {
				BussDelay c=new BussDelay(value);
				if (!c.How_Long_Delayed.equals("")){
					out.collect(new Tuple2<>(c.Boro,c));
				}
			}
		});


		//windowCoun.assignTimestampsAndWatermarks(x->time() );
		SingleOutputStreamOperator<Tuple2<String,BussDelay>> provaaa =
				windowCoun.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor
								<Tuple2<String, BussDelay>>(Time.days(10)) {
			@Override
			public long extractTimestamp(Tuple2<String,BussDelay> tup) {

				String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
				String ex = tup.f1.Occurred_On;

				SimpleDateFormat df = new SimpleDateFormat(format);
				Date dat = null;
				try {
					dat = df.parse(ex);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return dat.getTime();//bussDelay.How_Long_Delayed;
			}
		});
	//	KeyedStream<Tuple2<String, String>,Tuple> ddd = windowCoun.keyBy(1);

		KeyedStream<Tuple2<String, BussDelay>, Tuple> x = provaaa.keyBy(0);

		WindowedStream<Tuple2<String, BussDelay>, Tuple, TimeWindow> s = x.window((TumblingEventTimeWindows.of(Time.days(30))));

		SingleOutputStreamOperator<Tuple2<String, BussDelay>> k = s.reduce(new ReduceFunction<Tuple2<String, BussDelay>>() {
			@Override
			public Tuple2<String, BussDelay> reduce(Tuple2<String, BussDelay> tup1, Tuple2<String, BussDelay> tup2) throws Exception {
				//tup1.f1.Boro = tup1.f1.Boro + " " + tup2.f1.Boro;
				tup1.f1.count=tup1.f1.count+tup2.f1.count;
				//tup1.f1.Occurred_On=returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);
				tup1.f1.How_Long_Delayed=String.valueOf(Integer.valueOf(tup1.f1.How_Long_Delayed)+Integer.valueOf(tup2.f1.How_Long_Delayed));
				return new Tuple2<>(tup1.f0, tup1.f1);
			}
		});

		SingleOutputStreamOperator<Tuple3<String,String,String>> datafinale = k.map(
				new MapFunction<Tuple2<String, BussDelay>, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(Tuple2<String, BussDelay> tup) throws Exception {
						double avg=Double.valueOf(tup.f1.How_Long_Delayed) / Double.valueOf(tup.f1.count);
						return new Tuple3<String,String,String>(tup.f1.Boro,String.valueOf(avg),tup.f1.Occurred_On);
					}
		});


		datafinale.print().setParallelism(1);
		env.execute("Socket Window WordCount");

		/*
		s.reduce()
		s.reduce(new ReduceFunction, WindowFunction <BussDelay> {
			public BussDelay reduce(BussDelay f, BussDelay b){
				return f;
		}
		});/


		env.execute("Socket Window WordCount");
		/*windowCoun.timeWindowAll(Time.seconds(2));
		windowCoun.print().setParallelism(1);

*/
/*
		DataStream<WordWithCount> windowCounts = text

		.flatMap(new FlatMapFunction<String, WordWithCount>() {
			@Override
			public void flatMap(String value, Collector<WordWithCount> out) {
				for (String word : value.split("\\s")) {
					out.collect(new WordWithCount(word, 1L));
				}
			}
		})

				.keyBy("word")
				.timeWindow(Time.seconds(5))

				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		env.execute("Socket Window WordCount");*/
	}


	public static String returnMinDate(String date1, String date2){



		String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";

		SimpleDateFormat df = new SimpleDateFormat(format);
		Date dat1 = null;
		Date dat2 =null;
		try {
			dat1 = df.parse(date1);
			dat2 = df.parse(date2);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (dat1.getTime() > dat2.getTime()){
			return  date2;
		}
		return date1;
	}

	// ------------------------------------------------------------------------



	/**
	 * Data type for words with count.
	 */

}
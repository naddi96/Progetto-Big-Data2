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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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

		// parse the data, group it, window it, and aggregate the counts
		DataStream<BussDelay> windowCoun=text.flatMap( new FlatMapFunction<String, BussDelay>() {
			@Override
			public void flatMap(String value, Collector<BussDelay> out)
					throws Exception {
				BussDelay c=new BussDelay(value);
				if (!c.How_Long_Delayed.equals("")){
					out.collect(c);
				}
			}
		});

	//	KeyedStream<Tuple2<String, String>,Tuple> ddd = windowCoun.keyBy(1);
		windowCoun.timeWindowAll(Time.seconds(2));
		windowCoun.print().setParallelism(1);
		env.execute("Socket Window WordCount");

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

	// ------------------------------------------------------------------------



	/**
	 * Data type for words with count.
	 */
	public static class BussDelay {
		public String How_Long_Delayed;
		public String Boro;
		public String Occurred_On;

		public BussDelay() {}

		private static String ritornaMinuti(String m){
			try {


				if (m.matches("[0-9]+")) {
					return m;
				}
				if ((!m.contains(":") && m.toLowerCase().contains("m") && (m.contains("-") || m.contains("/")))) {

					String stri = "";
					if (m.contains("/")) stri = "/";
					if (m.contains("-")) stri = "-";
					String[] numebers = m.split(stri);
					if (numebers.length > 1) {
						String num1 = numebers[0].replaceAll("[^0-9]", "");
						String num2 = numebers[1].replaceAll("[^0-9]", "");
						int num11 = Integer.valueOf(num1);
						int num22 = Integer.valueOf(num2);

						double x = (num11 + num22) / 2.0;
						return String.valueOf(x);
					}
				}

				if ((!m.contains(":") && m.toLowerCase().contains("m") && !(m.contains("/") || m.contains("-")))) {
					return String.valueOf(m.replaceAll("[^0-9]", ""));
				}
				if ((!m.contains(":") && (!(m.toLowerCase().contains("m") && m.toLowerCase().contains("h"))) && (m.contains("-") || m.contains("/")))) {
					String stri = "";
					if (m.contains("/")) stri = "/";
					if (m.contains("-")) stri = "-";
					String[] numebers = m.split(stri);
					if (numebers.length > 1) {
						String num1 = numebers[0].replaceAll("[^0-9]", "");
						String num2 = numebers[1].replaceAll("[^0-9]", "");
						int num11 = Integer.valueOf(num1);
						int num22 = Integer.valueOf(num2);
						double x = (num11 + num22) / 2.0;
						return String.valueOf(x);
					}
				}
				if (m.toLowerCase().contains("h") && !(m.contains("-") && m.contains("/"))) {
					if (m.contains(":")) {
						;
						String[] x = m.split(":");
						Integer num1 = Integer.valueOf(x[0].replaceAll("[^0-9]", ""));
						Integer num2 = Integer.valueOf(x[1].replaceAll("[^0-9]", ""));
						return String.valueOf(num1 * 60 + num2);
					}
					int num11 = Integer.valueOf(m.replaceAll("[^0-9]", ""));

					return String.valueOf(num11 * 60);
				}
				if (m.contains("1/2")) {
					return "30";
				}
				if (m.contains("45min/1hr")) {
					return "105";
				}
				if (m.contains("35min/45mi")) {
					return "40";
				}

			}catch (Exception e){
				System.out.println(m);
			}
			return "";

		}
		public BussDelay(String line) {
			String[] x = line.split(";");
			this.Occurred_On=x[7];
			this.How_Long_Delayed=ritornaMinuti(x[11]).replaceAll("[^0-9]", "");
			this.Boro=x[9];
		}

		@Override
		public String toString() {
			return this.How_Long_Delayed + " : " + this.Boro + " : " + this.Occurred_On;
		}
	}
}
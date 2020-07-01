

package naddi.sadb.progetto.query1;

import naddi.sadb.progetto.KafkaConfig;
import naddi.sadb.progetto.utils.utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;



public class Query1 {



	public static void main(String[] args) throws Exception {

		//Time time = Time.days(1);
		Time time = Time.days(7);
		//Time time = Time.days(30);

		//final String hostname="127.0.0.1";
		//final int port =444;
		//DataStream<String> text = env.socketTextStream(hostname, port, "\n");


		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaConfig kafka=new KafkaConfig();
		Properties properties =kafka.getProperties();
		FlinkKafkaProducer<String> myProducer = kafka.getProducer();


		DataStream<String> text = env
				.addSource(new FlinkKafkaConsumer<>(kafka.input_topic, new SimpleStringSchema(), properties));

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




}
package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import org.classess.AggregateOwn;
import org.classess.BoundedOutOfOrdernessGenerator;
import org.classess.ControlFunction;
import org.structures.Eval;
import org.structures.Film;
import org.structures.Rate;
import org.structures.Score;

import java.util.Properties;

public class Consumer
{
    public static void main(final String[] args) throws Exception
    {
        int D = Integer.parseInt(args[0]);
        int L = Integer.parseInt(args[1]);
        double O = Double.valueOf(args[2]);

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        senv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1));
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.enableCheckpointing(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>("kafka-netflix", new SimpleStringSchema(), properties);
        kafkaSource.setStartFromLatest();

        String path = "/home/pawel/movie_titles.csv";
        DataStream<Film> films = senv.readTextFile(path)
                .filter(a -> !a.startsWith("ID"))
                .filter(a -> !a.contains("NULL"))
                .map(a -> a.split(","))
                .filter(a -> a.length == 3)
                .map(a -> new Film(Integer.parseInt(a[0]), Integer.parseInt(a[1]), a[2]))
                .keyBy(a -> a.film_id);

        DataStream<Rate> rates = senv.addSource(kafkaSource)
                .filter(a -> !a.startsWith("date"))
                .filter(a -> !a.contains("NULL")).map(a -> a.split(",")).filter(a -> a.length == 4).
                map(a -> new Rate(a[0], Integer.parseInt(a[1]), Integer.parseInt(a[2]), Integer.parseInt(a[3]))).keyBy(a -> a.film_id);

        DataStream<Score> scores = films.connect(rates)
                .keyBy(a -> a.film_id, b -> b.film_id)
                .flatMap(new ControlFunction()).
                assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        DataStream<Eval> evals = scores.keyBy(a -> a.date)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .aggregate(new AggregateOwn());
        DataStream<Eval> anomalies = scores.keyBy(a -> a.date)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(D), Time.seconds(1)))
                .aggregate(new AggregateOwn())
                .filter(a -> a.average >= O).filter(a -> a.count >= L);

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>("localhost:9092", "kafka-output", new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);

        FlinkKafkaProducer011<String> anomalyProducer = new FlinkKafkaProducer011<String>("localhost:9092", "kafka-anomalies", new SimpleStringSchema());
        anomalyProducer.setWriteTimestampToKafka(true);

        anomalies.map(new MapFunction<Eval, String>()
        {
            @Override
            public String map(Eval eval) throws Exception
            {
                return eval.title + "," + eval.year_prod + "," + eval.count + "," + eval.average;
            }
        }).rebalance().print();


        anomalies.map(new MapFunction<Eval, String>()
        {
            @Override
            public String map(Eval eval) throws Exception
            {
                return eval.title + "," + eval.year_prod + "," + eval.count + "," + eval.average;
            }
        }).addSink(anomalyProducer);


        evals.map(new MapFunction<Eval, String>()
        {
            @Override
            public String map(Eval eval) throws Exception
            {
                return eval.title + "," + eval.year_prod + "," + eval.count + "," + eval.average;
            }
        }).addSink(myProducer);

        senv.execute("Netflix prize datas");
    }
}

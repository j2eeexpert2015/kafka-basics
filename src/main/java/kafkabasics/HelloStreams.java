package kafkabasics;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

public class HelloStreams {
	
	public static void main(String[] args) {
		
		String brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
		String username = "cyy3wd7r";
		String password = "eAQPX5G290PkZ5CKo6drJKvqHqO6FA66";
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, username, password);
		String inputTopic = username + "-default";

		
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "HelloStreams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "SCRAM-SHA-256");
        config.put("sasl.jaas.config", jaasCfg);
        
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(inputTopic);
        source.foreach(new ForeachAction<String, String>() {
            public void apply(String key, String value) {
                System.out.println(key + ": " + value);
            }
         });
        
        

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
	}

}

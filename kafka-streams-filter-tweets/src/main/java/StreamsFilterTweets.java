import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.google.gson.JsonParser;

import java.util.Properties;

public class StreamsFilterTweets {


    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder= new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filtredStream = inputTopic.filter(
                (key,jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
        );
        filtredStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start our stream application
    }

    private static JsonParser jsonParser;

    private static Integer extractUserFollowersInTweet(String tweetJson){
        // gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }
}

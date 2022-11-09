import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;

public class EvenNumber {
    public static void main(String[] args) {

        int evenNumber;

        KafkaConsumer consumer;
        String topic = "hi";
        String broker = "localhost:9092";
        Properties props = new Properties();
        props.put("bootstrap.servers",broker);
        props.put("group.id", "test-group");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topic));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record: records){
                System.out.println(record.value());

                evenNumber = Integer.parseInt(record.value());
                if(evenNumber%2==0){
                    try{
                        Class.forName("com.mysql.jdbc.Driver");
                        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafkaevendb", "root","");
                        String sql = "INSERT INTO `evennumbers`(`evennumbers`) VALUES (?)";
                        PreparedStatement stmt = con.prepareStatement(sql);

                        stmt.setInt(1,evenNumber);
                        stmt.executeUpdate();
                        System.out.println("Even number inserted to db "+evenNumber);


                    }
                    catch (Exception e){
                        System.out.println(e);
                    }
                }

            }
        }
    }
}
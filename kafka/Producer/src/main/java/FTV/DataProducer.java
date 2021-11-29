package FTV;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Properties;
import java.util.UUID;
import java.io.IOException;
import java.util.stream.Stream;

// https://javapapers.com/core-java/monitor-a-folder-using-java/
public class DataProducer implements IConstants {

    private Producer<String, String> theproducer;
    public DataProducer(Properties pProperties) {
        this.theproducer = new KafkaProducer<String, String>(pProperties);
    }

    private void PublishMessages(String CsvFile) throws URISyntaxException{

        try{
            System.out.println(CsvFile);
            //URI uri = getClass().getClassLoader().getResource("/"+CsvFile).toURI();
            File file = new File(CsvFile);
            System.out.println(file.getAbsolutePath());
            Stream<String> FileStream = Files.lines(Paths.get("./fax/"+CsvFile));

            FileStream.forEach(line -> {
                
                StringBuilder sb = new StringBuilder();
                String[] s = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if(!s[1].equals("artist_name")){
                    sb.append(s[0]).append(";").append(s[1]).append(";").append(s[2]).append(";").append(s[3]).append(";").append(s[4]).append(";").append(s[5]).append(";").append(s[6]);
                    line = sb.toString();
                    final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                            TOPIC, UUID.randomUUID().toString(), line);

                    this.theproducer.send(csvRecord, (metadata, exception) -> {
                        if(metadata != null){
                            System.out.println("CsvData: -> "+ csvRecord.key()+" | "+ csvRecord.value());
                        }
                        else{
                            System.out.println("Error Sending Csv Record -> "+ csvRecord.value());
                        }
                    });
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException  {
        ///Users/danielbejarano/Desktop/BaseDeDatos2/Proyects/Bases2Proyecto2/fax
        Path faxFolder = Paths.get("./fax");
        WatchService watchService = FileSystems.getDefault().newWatchService();
        faxFolder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER+":"+KAFKA_PORT);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, APP_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        DataProducer kafkaProducer = new DataProducer(properties);

        boolean valid = true;
        do {
            WatchKey watchKey = watchService.take();

            for (WatchEvent event : watchKey.pollEvents()) {
                WatchEvent.Kind kind = event.kind();
                if (StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind())) {
                    String fileName = event.context().toString();
                    kafkaProducer.PublishMessages(fileName);
                    System.out.println("File Added:" + fileName);
                }
            }
            valid = watchKey.reset();

        } while (true);

    }
}
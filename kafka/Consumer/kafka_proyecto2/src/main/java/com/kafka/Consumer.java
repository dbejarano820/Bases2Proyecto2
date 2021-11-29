package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Consumer implements IConstants {

    public static void verifyDriver() throws ClassNotFoundException {
        Class.forName("org.mariadb.jdbc.Driver");
    }

    public static void main(String[] args) throws InterruptedException, SQLException {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER + ":" + KAFKA_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("processed_vibes"));
        System.out.println("SE SUSCRIBIO A PROCESSED_VIBES");

        try {
            verifyDriver();
            System.out.println("VERIFICO EL DRIVER");
            // String connection = String.format("jdbc:mariadb://%s:%s/%s", HOSTNAME, PORT,
            // DATABASE);
            // Connection conn = DriverManager.getConnection(connection, USER, PASSWORD);

            Connection conn = DriverManager
                    .getConnection("jdbc:mariadb://172.30.10.175:3306/feel_the_vibes?user=root&password=pamela1234");
            System.out.println("SE CONECTO A LA DB");
            while (true) {

                // limpiar todas las tablas
                //
                //

                System.out.println("consumer corriendo...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    // System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                    // se extrae la fila
                    System.out.println(record.toString());
                    if(record.value().equals("flush")){
                        //flush memsql
                        PreparedStatement stmt = conn
                                .prepareStatement("TRUNCATE TABLE questionA;");
                        stmt.executeQuery();
                        stmt = conn
                                .prepareStatement("TRUNCATE TABLE questionB;");
                        stmt.executeQuery();
                        stmt = conn
                                .prepareStatement("TRUNCATE TABLE questionC;");
                        stmt.executeQuery();
                        continue;
                    }


                    JSONObject row = new JSONObject(record.value());
                    System.out.println(row);
                    int year = Integer.parseInt(row.getString("year"));

                    // transforma los datos y los envia
                    if (row.getString("question").equals("c")) {
                        PreparedStatement stmt = conn
                                .prepareStatement("INSERT INTO questionC (year, themes) VALUES (?, ?)");
                        stmt.setInt(1, year);
                        stmt.setString(2, row.getString("themes"));
                        stmt.executeUpdate();
                    } else {
                        int positive = row.getInt("positive");
                        int negative =  row.getInt("negative");
                        int total =  row.getInt("total");
                        double ratio =  row.getDouble("ratio");
                        if (row.getString("question").equals("a")) {
                            PreparedStatement stmt = conn.prepareStatement(
                                    "INSERT INTO questionA (year, positive, negative, total, ratio) VALUES (?, ?, ?, ?, ?)");
                            stmt.setInt(1, year);
                            stmt.setInt(2, positive);
                            stmt.setInt(3, negative);
                            stmt.setInt(4, total);
                            stmt.setDouble(5, ratio);
                            stmt.executeUpdate();
                        } else if (row.getString("question").equals("b")) {
                            PreparedStatement stmt = conn.prepareStatement(
                                    "INSERT INTO questionB (genre, year, positive, negative, total, ratio) VALUES (?, ?, ?, ?, ?, ?)");
                            String genre = row.getString("genre");
                            stmt.setString(1, genre);
                            stmt.setInt(2, year);
                            stmt.setInt(3, positive);
                            stmt.setInt(4, negative);
                            stmt.setInt(5, total);
                            stmt.setDouble(6, ratio);
                            stmt.executeUpdate();
                        }
                    }
                }
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            consumer.close();
        }
    }
}

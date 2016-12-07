/**
 * Copyright 2015 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 * Licensed Materials - Property of IBM
 * (c) Copyright IBM Corp. 2015
 */
package com.messagehub.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.env.CreateTopicConfig;
import com.messagehub.samples.env.CreateTopicParameters;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;
import com.messagehub.samples.env.MessageList;

/**
 * Sample used for interacting with Message Hub over Secure Kafka / Kafka Native
 * channels.
 *
 * @author IBM
 */
public class MessageHubJavaSample {

    private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static final long HOUR_IN_MILLISECONDS = 3600000L;
    private static final Logger logger = Logger.getLogger(MessageHubJavaSample.class);
    private static String userDir, resourceDir;
    private static boolean isDistribution;
    private String topic="usa";
    private String kafkaHost = "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,kafka05-prod01.messagehub.services.us-south.bluemix.net:9093";
    //private String restHost = null;
    private String apiKey = null;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ArrayList<String> topicList;
    private boolean closing;
 
    public MessageHubJavaSample(String topicName){
    	 this.topic=topicName;
         String vcapServices = System.getenv("VCAP_SERVICES");
         ObjectMapper mapper = new ObjectMapper();
         System.out.println("vcapServices::" +vcapServices);
         System.setProperty("java.security.auth.login.config", "");
         
         if(vcapServices != null) {
             try {
                 // Parse VCAP_SERVICES into Jackson JsonNode, then map the 'messagehub' entry
                 // to an instance of MessageHubEnvironment.
                 JsonNode vcapServicesJson = mapper.readValue(vcapServices, JsonNode.class);
                 ObjectMapper envMapper = new ObjectMapper();
                 String vcapKey = null;
                 Iterator<String> it = vcapServicesJson.fieldNames();

                 // Find the Message Hub service bound to this application.
                 while (it.hasNext() && vcapKey == null) {
                     String potentialKey = it.next();

                     if (potentialKey.startsWith("messagehub")) {
                         logger.log(Level.INFO, "Using the '" + potentialKey + "' key from VCAP_SERVICES.");
                         vcapKey = potentialKey;
                     }
                 }

                 if (vcapKey == null) {
                     logger.log(Level.ERROR,
                                "Error while parsing VCAP_SERVICES: A Message Hub service instance is not bound to this application.");
                     return;
                 }
                 
                 MessageHubEnvironment messageHubEnvironment = envMapper.readValue(vcapServicesJson.get(vcapKey).get(0).toString(), MessageHubEnvironment.class);
                 MessageHubCredentials credentials = messageHubEnvironment.getCredentials();

                 kafkaHost = credentials.getKafkaBrokersSasl()[0];
                // restHost = credentials.getKafkaRestUrl();
                 apiKey = credentials.getApiKey();

                 System.out.println("kafkaHost::  "+kafkaHost + "apiKey:: " +apiKey);
                 //updateJaasConfiguration(credentials);
             } catch(final Exception e) {
                 e.printStackTrace();
                 return;
             }
         }
         
         if(apiKey==null){
        	 apiKey="4vyQ0exjKidRtclzehEWN0UFoNWuXDCsjkVKBmGqYQn1MUKK";
         }

         //Consumer Code
         closing = false;
         topicList = new ArrayList<String>();

         // Provide configuration and deserialisers
         // for the key and value fields received.
         kafkaConsumer = new KafkaConsumer<byte[], byte[]>(
                 getClientConfiguration(kafkaHost, apiKey, false),
                 new ByteArrayDeserializer(), new ByteArrayDeserializer());

         System.out.println("+++++++++++++++++++++++++Consumer Subscribing to topic:: " +topic);
         topicList.add(topic);
         kafkaConsumer.subscribe(topicList);
     }
    
 
      //public static void main(String args[]) throws InterruptedException,
      public void ConsumeData() throws InterruptedException,
            ExecutionException, IOException {
          while (!closing) {
          	System.out.println("Inside While to read message from Kafka");
              try {
                  // Poll on the Kafka consumer every second.
                  Iterator<ConsumerRecord<byte[], byte[]>> it = kafkaConsumer
                          .poll(1000).iterator();

                  // Iterate through all the messages received and print their
                  // content.
                  // After a predefined number of messages has been received, the
                  // client
                  // will exit.
                  while (it.hasNext()) {
                      ConsumerRecord<byte[], byte[]> record = it.next();
                      final String message = new String(record.value(),
                              Charset.forName("UTF-8"));
                      System.out.println("Message:: " +message);
                      logger.log(Level.INFO, "Message: " + message);
                  }

                  kafkaConsumer.commitSync();

                  Thread.sleep(1000);
              } catch (final InterruptedException e) {
                  logger.log(Level.ERROR, "Producer/Consumer loop has been unexpectedly interrupted");
                  shutdown();
              } catch (final Exception e) {
                  logger.log(Level.ERROR, "Consumer has failed with exception: " + e);
                  shutdown();
              }
          }
          kafkaConsumer.close();
     
      }

      public void shutdown() {
              closing = true;
          }
 
    /**
     * Retrieve client configuration information, using a properties file, for
     * connecting to secure Kafka.
     *
     * @param broker
     *            {String} A string representing a list of brokers the producer
     *            can contact.
     * @param apiKey
     *            {String} The API key of the Bluemix Message Hub service.
     * @param isProducer
     *            {Boolean} Flag used to determine whether or not the
     *            configuration is for a producer.
     * @return {Properties} A properties object which stores the client
     *         configuration info.
     */
    public final Properties getClientConfiguration(String broker,
            String apiKey, boolean isProducer) {
        Properties props = new Properties();
        InputStream propsStream;
        String fileName= "resources/consumer.properties";

        try {
        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        	propsStream = classLoader.getResourceAsStream(fileName);
            props.load(propsStream);
            propsStream.close();
        } catch (IOException e) {
            logger.log(Level.ERROR, "Could not load properties from file");
            return props;
        }

        System.out.println("+++++++++++++++++++++++++++bootstrap.servers:: " +broker);
        props.put("bootstrap.servers", broker);

        return props;
    }
}

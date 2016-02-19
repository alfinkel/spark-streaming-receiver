package com.ibm.spark.streaming

import scala.collection.JavaConversions._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.kafka.common.security.JaasUtils
import java.io.FileWriter
import java.io.File

object SparkMHReceiver extends Logging {
  
  var ssc: StreamingContext = null
  
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming with MessageHub")
    val sc = new SparkContext(conf)
    startMessageHubStreaming(sc, "eN1qmxY3qe0TqSie", "qmSVDFYuAGhqQOWWyg3GKJsK8eILeqAV");
  }

  def startMessageHubStreaming( sc: SparkContext, username: String, password: String, sec: Int=5) {

    createJaasConfiguration(username, password)
    val servers = "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093," +
                  "kafka02-prod01.messagehub.services.us-south.bluemix.net:9093," +
                  "kafka03-prod01.messagehub.services.us-south.bluemix.net:9093," +
                  "kafka04-prod01.messagehub.services.us-south.bluemix.net:9093," +
                  "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093"
    val ssc = new StreamingContext(sc, Seconds(sec))
    val mhParams = Map("bootstrap.servers" -> servers,
                       "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                       "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                       "client.id" -> "message-hub-sample",
                       "enable.auto.commit" -> "false",
                       "auto.offset.reset" -> "latest",
                       "security.protocol" -> "SASL_SSL",
                       "ssl.protocol" -> "TLSv1.2",
                       "ssl.enabled.protocols" -> "TLSv1.2",
                       "ssl.truststore.location" -> getDefaultSSLTrustStoreLocation(),
                       "ssl.truststore.password" -> "changeit",
                       "ssl.truststore.type" -> "JKS",
                       "ssl.endpoint.identification.algorithm" -> "HTTPS")
    val topics = List("mytopic")
    val receiver = new CustomReceiver(mhParams, topics)
    val lines = ssc.receiverStream(receiver)
    val counts = lines.count()
    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  private def fixPath(path: String):String = {
    path.replaceAll("\\ / : * ? \" < > |,", "_")
  }

  private def getDefaultSSLTrustStoreLocation():String={
    val javaHome = System.getProperty("java.home") + File.separator + "lib" + File.separator + "security" + File.separator + "cacerts"
    println("default location of ssl Trust store is: " + javaHome)
    javaHome
  }

  def createJaasConfiguration( userName: String, password: String) {
    //Create the jaas configuration
    try{
      val confString = """KafkaClient {
                         |     com.ibm.messagehub.login.MessageHubLoginModule required
                         |     serviceName="kafka"
                         |     username="$USERNAME"
                         |     password="$PASSWORD";
                         |};""".stripMargin.replace("$USERNAME", userName).replace("$PASSWORD", password)
      
      val confDir= new File( System.getProperty("java.io.tmpdir") + File.separator + 
          fixPath( userName ) )
      confDir.mkdirs
      val confFile = new File( confDir, "jaas.conf");
      val fw = new FileWriter( confFile );
      fw.write( confString )
      fw.close

      println(confString)
      
      //Set the jaas login config property
      println("Registering JaasConfiguration: " + confFile.getAbsolutePath)
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, confFile.getAbsolutePath )
    }catch{
      case e:Throwable => {
        e.printStackTrace
        throw e
      }        
    }
  }
}

class CustomReceiver(mhParams: Map[String,String], topics: List[String]) 
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) with Logging {

  // Connection to Kafka
  var kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  def onStop() {
    if (kafkaConsumer != null) {
      kafkaConsumer.synchronized {
        print("Stopping kafkaConsumer")
        kafkaConsumer.close()
        kafkaConsumer = null
      }
    }
  }

  def onStart() {
    logInfo("Starting Kafka Consumer Stream")
    
    //Create a new kafka consumer and subscribe to the relevant topics
    kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](mhParams, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    kafkaConsumer.subscribe(topics)
    
    new Thread( new Runnable {
      def run(){
        try{
          while( kafkaConsumer != null ){
            var it:Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null;
            
            if ( kafkaConsumer != null ){
              kafkaConsumer.synchronized{     
                //Poll for new events
                it = kafkaConsumer.poll(1000L).iterator              
                while( it != null && it.hasNext() ){
                  //Get the record and store it
                  val record = it.next();
                  store( new String(record.value) )
                }            
                kafkaConsumer.commitSync
              }
            }
            Thread.sleep( 1000L )
          }  
          println("Exiting Thread")
        }catch{
          case e:Throwable => {
            reportError( "Error in KafkaConsumer thread", e);
            e.printStackTrace()
          }
        }
      }
    }).start
  }
}
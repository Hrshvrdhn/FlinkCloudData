package code.data.scala.flink

import java.io.FileNotFoundException
import java.util.Properties

object FlinkRedis {

  case class parkingSlot(deviceID: String, parkingSlotAvailability: Int)

  def main(args: Array[String]) {
    val TOPIC = "inputData"

    try {


      val properties = new Properties()
      properties.setProperty("bootstrap.servers", "<host>:<port>")
      properties.setProperty("group.id", "FlinkExampleProducer")
      properties.setProperty("request.timeout.ms", "60000")
      properties.setProperty("sasl.mechanism", "PLAIN")
      properties.setProperty("security.protocol", "SASL_SSL")
      properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"<connection_endpoint>\";")

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val flinkConsumer = new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), properties)
      flinkConsumer.setStartFromLatest()
      val stream = env.addSource(flinkConsumer)


//      val stream = env
//        .addSource(
//          new FlinkKafkaConsumer("DemoK",
//            // new JSONKeyValueDeserializationSchema(false),
//            new SimpleStringSchema(),
//            properties)
//          //.setStartFromEarliest()
//        )

      val flinkRecords = stream.map(x => {
        val input = JSON.parseFull(x)
//          .getOrElse(Map("did" -> "not a valid json", "pav" -> 0))
//          .asInstanceOf[Map[String, Object]]

//        parkingSlot(input("body").toString, input("pav").toString.toDouble.toInt)
        input.toString

      })

      val conf = new FlinkJedisPoolConfig.Builder()
        .setHost("<host-name>")
        .setPort(6379)
        .setPassword("<access_token>")
        .build()

      val mappedRecs = flinkRecords.map(ps => (ps,"demoR"))

      mappedRecs.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))
      flinkRecords.print()


      env.execute("Flink Kafka Example")
    }catch {
      case e: FileNotFoundException =>
        println("FileNoteFoundException: " + e)
      case e: Exception =>
        println("Failed with exception " + e)
    }
  }

  class RedisExampleMapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "DemoRFlink")
    }

    override def getKeyFromData(data: (String, String)): String = data._2

    override def getValueFromData(data: (String, String)): String = data._1
  }

}

package cn.hyr.user.etl.service

import java.util.Properties

import cn.hyr.user.etl.common.Constants
import cn.hyr.user.etl.utils.{JedisClusterProxy, JedisPoolUtils, KafkaSink}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** *****************************************************************************
  * @date 2019-08-31 12:30
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 用户ID去重 etl
  * *****************************************************************************/
object UserIdDuplicateRemovalService {

  def run(): Unit = {
    val sparkConf = new SparkConf().setAppName("JavaKafkaDirectWordCount").setMaster("local[1]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // TODO 使用bitmap来存放数值型数据 判断某个用户id是否存在

    // 构建kafka连接时的参数信息
    //    val kafkaParams = new util.HashMap[String, String]
    //    kafkaParams.put("metadata.broker.list", "master:9092") // 指定broker在哪
    //    kafkaParams.put("group.id", "weibo_etl_") // 指定broker在哪
    //    kafkaParams.put("auto.offset.reset", "smallest") // 指定broker在哪

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "master:9092",
      "group.id" -> "weibo_etl_",
      "auto.offset.reset" -> "smallest")

    // Create direct kafka stream with brokers and topics createDirectStream()
    val brokers = "master:9092,slave1:9092,slave2:9092"
    val topics = "WEIBO_USER_ID"
    val topicsSet = topics.split(",").toSet

    // Create direct kafka stream with brokers and topics
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //利用广播变量的形式，将KafkaProducer和redis广播到每一个executor，如下：
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        //待确认broker的key是否: metadata.broker.list
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092")
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        p
      }
      println("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val jedis: Broadcast[JedisClusterProxy] = {
      // 连接redis 存放用户id
      JedisPoolUtils.initJedisCluster()
      println("redis init done!")
      ssc.sparkContext.broadcast(JedisPoolUtils.getPool)
    }

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // val sparkSession = SparkSession.builder().getOrCreate()
        rdd.map(t => {
          val user_id = t._2
          if (!jedis.value.exists(Constants.USER_ID_PREFIX + user_id)) { // 如果不存在
            println("user_id:" + user_id)
            // 存储
            jedis.value.set(Constants.USER_ID_PREFIX + user_id, user_id)
            // 发送到kafka
            kafkaProducer.value.send("WEIBO_USER_ID_RESULT", user_id)
            println("发送成功")
          }
        }).collect()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }


}

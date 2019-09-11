package spark

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object M_PolicyCreditApp {

  def gainRiskRank(rate: Double):String = {
    var result = ""
    if(rate >=0.75 && rate <0.8){
      result="R1"
    }else if(rate >=0.8 && rate<=1){
      result="R2"
    }else{
      result="G1"
    }
    result
  }

  def main(args: Array[String]): Unit = {
    //
    System.out.println("测试"+PropertiesUtil.getProString("spark.master"))
    println("测试"+PropertiesUtil.getProString("spark.app.name"))
    println("测试"+PropertiesUtil.getProString("spark.app.name"))
    val conf =new SparkConf()
      .setMaster(PropertiesUtil.getProString("spark.master"))
      .setAppName(PropertiesUtil.getProString("spark.app.name"))
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //如果环境中
    val ssc = new StreamingContext(conf,Seconds(PropertiesUtil.getProInt("spark.streaming.durations.sec").toLong))
    ssc.checkpoint(PropertiesUtil.getProString("spark.checkout.dir"))
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> PropertiesUtil.getProString("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id" -> PropertiesUtil.getProString("group.id"),
      "auto.offset.reset" -> PropertiesUtil.getProString("auto.offset.reset"),
      "enable.auto.commit" ->(PropertiesUtil.getProBoolean("enable.auto.commit"):java.lang.Boolean)
    )
    val topics = Array(PropertiesUtil.getProString("kafka.topic.name"))
    val kafkaStreaming = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    kafkaStreaming.map[JSONObject](line =>{
      println("$$$\t" +line.value())
      JSON.parseObject(line.value)
    }).filter(jsonObj =>{
      if(null == jsonObj || !"canal_test".equals(jsonObj.getString("database"))){
        false
      }else{
        val chType = jsonObj.getString("type")
        if("INSERT".equals(chType) || "UPDATE".equals(chType)){
          true
        }else{
          false
        }
      }
    }).flatMap[(JSONObject,JSONObject)](jsonObj =>{
      var oldJsonArr:JSONArray = jsonObj.getJSONArray("old")
      var dataJsonArr:JSONArray = jsonObj.getJSONArray("data")
      if("INSERT".equals(jsonObj.getString("type"))){
        oldJsonArr = new JSONArray()
        val oldJsonObj2 = new JSONObject()
        oldJsonObj2.put("mor_rate","0")
        oldJsonArr.add(oldJsonObj2)
      }
      val result = ListBuffer[(JSONObject,JSONObject)]()
      for(i <- 0 until oldJsonArr.size()){
        val jsonTuple =(oldJsonArr.getJSONObject(i),dataJsonArr.getJSONObject(i))
        result += jsonTuple
      }
      result
    }).map(t=>{
      val p_num = t._2.getString("p_num")
      val nowMorRate = t._2.getString("mor_rate").toDouble
      val chMorRate = nowMorRate - t._1.getDouble("mor_rate")
      val riskRank = gainRiskRank(nowMorRate)
      (p_num,riskRank,nowMorRate,chMorRate,new java.util.Date)
    }).foreachRDD(rdd=>{
      rdd.foreachPartition(p =>{
        val paramsList = ListBuffer[ParamsList]()
        val jdbcWrapper = JDBCWrapper.getInstance()
        while(p.hasNext){
          val record = p.next()
          val paramsListTmp = new ParamsList
          paramsListTmp.p_num= record._1
          paramsListTmp.risk_rank = record._2
          paramsListTmp.mor_rate = record._3
          paramsListTmp.ch_mor_rate =record._4
          paramsListTmp.load_time = record._5
          paramsListTmp.params_Type = "real_risk"
          paramsList += paramsListTmp
        }
        val insertNum = jdbcWrapper.doBatch("INSERT INTO real_risk VALUE(?,?,?,)",paramsList)
        print("INSERT INTO real_risk:" + insertNum.mkString(","))
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

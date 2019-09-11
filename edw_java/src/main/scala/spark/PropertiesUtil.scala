package spark

import java.util.Properties

object PropertiesUtil {
  private val properties:Properties =new Properties()
  def getProperties(): Properties ={
    if(properties.isEmpty){
      val reader =getClass.getResourceAsStream("application.properties")
      properties.load(reader)
    }
    properties
  }

  /**
   * 获取配置文件keY对外的字符串值
 *
   * @param key
   */
  def getProString(key:String): String ={
    getProperties().getProperty(key)
  }

  /**
   * 获取配置文件中key对应的布尔值
   * @param key
   * @return
   */
  def getProInt(key:String): Int ={
    getProperties().getProperty(key).toInt
  }

  /***
   * 获取配置文件中key对应的布尔值
   * @param key
   * @return
   */
  def getProBoolean(key:String): Boolean ={
    getProperties().getProperty(key).toBoolean
  }
}

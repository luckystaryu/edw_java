package spark

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.mutable.ListBuffer

object JDBCWrapper {
  private var jdbcInstance: JDBCWrapper = _

  def getInstance(): JDBCWrapper = {
    if (jdbcInstance == null) {
      synchronized {
        if (jdbcInstance == null) {
          jdbcInstance = new JDBCWrapper()
        }
      }
    }
    jdbcInstance
  }
  jdbcInstance
}

class JDBCWrapper {
     //连接池的大小
  val POOL_SIZE:Int = PropertiesUtil.getProInt("jdbc.datasource.size")
  val dbConnnectionPool = new LinkedBlockingDeque[Connection](POOL_SIZE)
  try
    Class.forName(PropertiesUtil.getProString("jdbc.driver"))
  catch {
    case e:ClassNotFoundException => e.printStackTrace()
  }
  for(i <- 0 until POOL_SIZE){
    try{
      val conn = DriverManager.getConnection(
        PropertiesUtil.getProString("jdbc.url"),
        PropertiesUtil.getProString("jdbc.user"),
        PropertiesUtil.getProString("jdbc.password")
      );
      dbConnnectionPool.put(conn)
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
  def getConnection():Connection =synchronized {
    while (0 == dbConnnectionPool.size()) {
      try {
        Thread.sleep(20)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }
      dbConnnectionPool.poll()
  }
    def doBatch(sqlText:String,paramsList:ListBuffer[ParamsList]):Array[Int]={
      val conn:Connection = getConnection()
      var ps:PreparedStatement = null
      var result:Array[Int] =null
      try{
        conn.setAutoCommit(false)
        ps = conn.prepareStatement(sqlText)
        for(paramters <- paramsList){
          paramters.params_Type match {
            case "real_risk" =>{
              println("$$$\treal_risk\t"+ paramsList)
              ps.setObject(1,paramters.p_num)
              ps.setObject(2,paramters.risk_rank)
              ps.setObject(3,paramters.mor_rate)
              ps.setObject(4,paramters.ch_mor_rate)
              ps.setObject(5,paramters.load_time)
            }
          }
          ps.addBatch()
        }
        result = ps.executeBatch()
        conn.commit()
      }catch {
        case e:Exception =>e.printStackTrace()
      }finally {
        if(ps !=null)
          try{
            ps.close()
          }catch {
            case e:SQLException =>e.printStackTrace()
          }
        if(conn !=null)
          try{
            dbConnnectionPool.put(conn)
          }catch {
            case e:InterruptedException => e.printStackTrace()
          }
      }
      result
    }
}

package spark

class ParamsList extends Serializable{
  var p_num:String = _
  var risk_rank:String = _
  var mor_rate:Double = _
  var ch_mor_rate:Double = _
  var load_time:java.util.Date =_
  var params_Type :String = _

  override def toString: String = s"ParamsList($p_num,$risk_rank,$mor_rate,$ch_mor_rate,$load_time)"
}

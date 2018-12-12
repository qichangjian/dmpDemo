package com.qcj.bigdata.dmp.entity

import com.qcj.bigdata.dmp.util.Utils
import org.apache.commons.lang3.StringUtils

/**
  * Bean实体，对应data数据
  */
//Product ：数据最终转换为元组，但是元组大小最大是22所以提供了Product来扩展元组
class Logs (val sessionid: String, //会话标识
            val advertisersid: Int, //广告主id
            val adorderid: Int, //广告id
            val adcreativeid: Int, //广告创意id   ( >= 200000 : dsp ,  < 200000 oss)
            val adplatformproviderid: Int, //广告平台商id  (>= 100000: rtb  , < 100000 : api )
            val sdkversionnumber: String, //sdk版本号
            val adplatformkey: String, //平台商key
            val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
            val requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
            val adprice: Double, //广告价格
            val adppprice: Double, //平台商价格
            val requestdate: String, //请求时间,格式为：yyyy-m-dd hh:mm:ss
            val ip: String, //设备用户的真实ip地址
            val appid: String, //应用id
            val appname: String, //应用名称
            val uuid: String, //设备唯一标识，比如imei或者androidid等
            val device: String, //设备型号，如htc、iphone
            val client: Int, //设备类型 （1：android 2：ios 3：wp）
            val osversion: String, //设备操作系统版本，如4.0
            val density: String, //备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2
            val pw: Int, //设备屏幕宽度
            val ph: Int, //设备屏幕高度
            val longitude: String, //设备所在经度
            val lat: String, //设备所在纬度
            val provincename: String, //设备所在省份名称
            val cityname: String, //设备所在城市名称
            val ispid: Int, //运营商id
            val ispname: String, //运营商名称
            val networkmannerid: Int, //联网方式id
            val networkmannername: String, //联网方式名称
            val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
            val isbilling: Int, //是否收费（0：未收费 1：已收费）
            val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
            val adspacetypename: String, //广告位类型名称（banner、插屏、全屏）
            val devicetype: Int, //设备类型（1：手机 2：平板）
            val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
            val apptype: Int, //应用类型id
            val district: String, //设备所在县名称
            val paymode: Int, //针对平台商的支付模式，1：展示量投放(CPM) 2：点击量投放(CPC)
            val isbid: Int, //是否rtb
            val bidprice: Double, //rtb竞价价格
            val winprice: Double, //rtb竞价成功价格
            val iswin: Int, //是否竞价成功
            val cur: String, //values:usd|rmb等
            val rate: Double, //汇率
            val cnywinprice: Double, //rtb竞价成功转换成人民币的价格
            val imei: String, //imei
            val mac: String, //mac
            val idfa: String, //idfa
            val openudid: String, //openudid
            val androidid: String, //androidid
            val rtbprovince: String, //rtb 省
            val rtbcity: String, //rtb 市
            val rtbdistrict: String, //rtb 区
            val rtbstreet: String, //rtb 街道
            val storeurl: String, //app的市场下载地址
            val realip: String, //真实ip
            val isqualityapp: Int, //优选标识
            val bidfloor: Double, //底价
            val aw: Int, //广告位的宽
            val ah: Int, //广告位的高
            val imeimd5: String, //imei_md5
            val macmd5: String, //mac_md5
            val idfamd5: String, //idfa_md5
            val openudidmd5: String, //openudid_md5
            val androididmd5: String, //androidid_md5
            val imeisha1: String, //imei_sha1
            val macsha1: String, //mac_sha1
            val idfasha1: String, //idfa_sha1
            val openudidsha1: String, //openudid_sha1
            val androididsha1: String, //androidid_sha1
            val uuidunknow: String, //uuid_unknow tanx密文
            val decuuidunknow: String, // 解密的tanx 明文
            val userid: String, //平台用户id
            val reqdate: String, //日期
            val reqhour: String, //小时
            val iptype: Int, //表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1
            val initbidprice: Double, //初始出价
            val adpayment: Double, //转换后的广告消费（保留小数点后6位）
            val agentrate: Double, //代理商利润率
            val lomarkrate: Double, //代理利润率
            val adxrate: Double, //媒介利润率
            val title: String, //标题
            val keywords: String, //关键字
            val tagid: String, //广告位标识(当视频流量时值为视频ID号)
            val callbackdate: String, //回调时间 格式为:YYYY/mm/dd hh:mm:ss
            val channelid: String, //频道ID
            val mediatype: Int )//媒体类型：1长尾媒体 2视频媒体 3独立媒体  默认:1)
  extends Product with Serializable {
  //继承Product重写的三个方法
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversionnumber
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => longitude
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => decuuidunknow
    case 73 => userid
    case 74 => reqdate
    case 75 => reqhour
    case 76 => iptype
    case 77 => initbidprice
    case 78 => adpayment
    case 79 => agentrate
    case 80 => lomarkrate
    case 81 => adxrate
    case 82 => title
    case 83 => keywords
    case 84 => tagid
    case 85 => callbackdate
    case 86 => channelid
    case 87 => mediatype
  }
  override def productArity: Int = 88//类中多少字段
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Logs]
}
//伴生对象
//有些字段是衍生字段（生日，年龄可以通过生日计算出来
object Logs{
  //伴生对象的方法
  def line2Logs(line:String):Logs={
    if(StringUtils.isNotEmpty(line)){
      val fields = line.split(",")
      if(fields.length >= 79){
        new Logs(fields(0), Utils.parseInt(fields(1)), Utils.parseInt(fields(2)), Utils.parseInt(fields(3)), Utils.parseInt(fields(4)), fields(5), fields(6), Utils.parseInt(fields(7)), Utils.parseInt(fields(8)), Utils.parseDouble(fields(9)), Utils.parseDouble(fields(10)),
          fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), Utils.parseInt(fields(17)), fields(18), fields(19), Utils.parseInt(fields(20)),
          Utils.parseInt(fields(21)), fields(22), fields(23), fields(24), fields(25), Utils.parseInt(fields(26)), fields(27), Utils.parseInt(fields(28)), fields(29), Utils.parseInt(fields(30)),
          Utils.parseInt(fields(31)), Utils.parseInt(fields(32)), fields(33), Utils.parseInt(fields(34)), Utils.parseInt(fields(35)), Utils.parseInt(fields(36)), fields(37), Utils.parseInt(fields(38)), Utils.parseInt(fields(39)), Utils.parseDouble(fields(40)),
          Utils.parseDouble(fields(41)), Utils.parseInt(fields(42)), fields(43), Utils.parseDouble(fields(44)), Utils.parseDouble(fields(45)), fields(46), fields(47), fields(48), fields(49), fields(50),
          fields(51), fields(52), fields(53), fields(54), fields(55), fields(56), Utils.parseInt(fields(57)), Utils.parseDouble(fields(58)), Utils.parseInt(fields(59)), Utils.parseInt(fields(60)),
          fields(61), fields(62), fields(63), fields(64), fields(65), fields(66), fields(67), fields(68), fields(69), fields(70),
          fields(71), "", fields(72), Utils.fmtDate(fields(11)).getOrElse("unkown"), Utils.fmtHour(fields(11)).getOrElse("unkown"),
          Utils.parseInt(fields(73)), Utils.parseDouble(fields(74)), Utils.parseDouble(fields(75)), Utils.parseDouble(fields(76)), Utils.parseDouble(fields(77)), Utils.parseDouble(fields(78)), "", "", "", "", "", 1)
      }else {
        makeLogs()
      }
    }else{
        makeLogs()
    }
  }

  //生成一个空对象
  def makeLogs(): Logs ={
    new Logs("", 0, 0, 0, 0, "", "", 0, 0, 0.0, 0.0, "", "", "", "", "", "", 0, "",
      "", 0, 0, "", "", "", "", 0, "", 0, "", 0, 0, 0, "", 0, 0, 0, "", 0, 0,
      0.0, 0.0, 0, "", 0.0, 0.0, "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0, 0,
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "", 0
    )
  }
}
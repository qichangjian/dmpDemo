package com.qcj.bigdata.dmp.jobs.constants;

/**
 * 为了避免硬编码，将标签前缀都变成常量
 */
public interface TagsConstants {
    String PRE_TAG_AD_POSTION = "LC_";//广告位标签前缀常量
    String PRE_TAG_APPNAME = "APP_";//APP标签前缀
    String PRE_TAG_CHANNEL = "CN_";//渠道标签前缀
    String PRE_TAG_DEVICE_OS = "DEVICE_OS_";//设备操作系统前缀
    String PRE_TAG_DEVICE_NETWORK = "DEVICE_NETWORK_";//设备联网方式前缀
    String PRE_TAG_DEVICE_ISP = "DEVICE_ISP_";//设备运营商方案前缀
    String PRE_TAG_KEYWORD = "KEYWORD_";//关键字标签前缀
    String PRE_TAG_ZONE_PROVINCE = "ZP_";//地域标签:省标签格式前缀
    String PRE_TAG_ZONE_CITY = "ZC_";//上下文标签前缀
}

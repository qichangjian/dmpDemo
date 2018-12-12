create database if not exists dmp_1807;
use dmp_1807;

---------------------------------------------------------
--地域分布
create table if not exists r_1807_arewa(
  `date` date,
  province varchar(30),
  city varchar(30),
    request_num_total BIGINT,
  request_num_valid BIGINT,
  request_num_ad  BIGINT,
  bid_num_participate BIGINT COMMENT '参与竞价数',
  bid_num_success BIGINT COMMENT '竞价成功数',
  ad_num_show BIGINT COMMENT '展示量',
  ad_num_click BIGINT COMMENT '点击量',
  ad_cost DOUBLE(8, 2) COMMENT '广告消费',
  ad_payment DOUBLE(8, 2) COMMENT '广告成本'
);

---------------------------------------------------------------
--终端设备：运营商
create table IF NOT EXISTS r_1807_ispname (
  `date` date,
  ispname varchar(30) COMMENT '运营商',
  request_num_total BIGINT,
  request_num_valid BIGINT,
  request_num_ad  BIGINT,
  bid_num_participate BIGINT COMMENT '参与竞价数',
  bid_num_success BIGINT COMMENT '竞价成功数',
  ad_num_show BIGINT COMMENT '展示量',
  ad_num_click BIGINT COMMENT '点击量',
  ad_cost DOUBLE(8, 2) COMMENT '广告消费',
  ad_payment DOUBLE(8, 2) COMMENT '广告成本'
);

---------------------------------------------------------------
--终端设备：联网方式
create table IF NOT EXISTS r_1807_networkmannername (
  `date` date,
  networkmannername varchar(30) COMMENT '联网方式',
  request_num_total BIGINT,
  request_num_valid BIGINT,
  request_num_ad  BIGINT,
  bid_num_participate BIGINT COMMENT '参与竞价数',
  bid_num_success BIGINT COMMENT '竞价成功数',
  ad_num_show BIGINT COMMENT '展示量',
  ad_num_click BIGINT COMMENT '点击量',
  ad_cost DOUBLE(8, 2) COMMENT '广告消费',
  ad_payment DOUBLE(8, 2) COMMENT '广告成本'
);

---------------------------------------------------------------
--终端设备：设备类型
create table IF NOT EXISTS r_1807_client (
  `date` date,
  client varchar(30) COMMENT '设备类型',
  request_num_total BIGINT,
  request_num_valid BIGINT,
  request_num_ad  BIGINT,
  bid_num_participate BIGINT COMMENT '参与竞价数',
  bid_num_success BIGINT COMMENT '竞价成功数',
  ad_num_show BIGINT COMMENT '展示量',
  ad_num_click BIGINT COMMENT '点击量',
  ad_cost DOUBLE(8, 2) COMMENT '广告消费',
  ad_payment DOUBLE(8, 2) COMMENT '广告成本'
);

---------------------------------------------------------------
--终端设备：媒体类型
create table IF NOT EXISTS r_1807_mediatype (
  `date` date,
  mediatype varchar(30) COMMENT '媒体类型',
  request_num_total BIGINT,
  request_num_valid BIGINT,
  request_num_ad  BIGINT,
  bid_num_participate BIGINT COMMENT '参与竞价数',
  bid_num_success BIGINT COMMENT '竞价成功数',
  ad_num_show BIGINT COMMENT '展示量',
  ad_num_click BIGINT COMMENT '点击量',
  ad_cost DOUBLE(8, 2) COMMENT '广告消费',
  ad_payment DOUBLE(8, 2) COMMENT '广告成本'
);



------------------------------------------------------------
--用户画像 ：
-- 1.-- 创建用户表
CREATE TABLE `t_user` (
  `uid` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) NOT NULL,
  `gender` tinyint(4) DEFAULT NULL,
  `idcard` varchar(20) DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `birthday` date DEFAULT NULL,
  `cid` int(11) DEFAULT NULL COMMENT '城市id',
  `email` varchar(50) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
--插入两条测试数据
INSERT INTO `t_user` VALUES (1, '汤其', 0, '371323199601274033', '17863860446', '1997-01-01', 0, 'tangqi@163.com', '曲阜');
INSERT INTO `t_user` VALUES (2, '孙玉', 1, '371323199601274128', '17843860331', '1998-02-22', 1, '178643@qq.com', '临沂');


package com.qcj.bigdata.dmp.util.db;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * 关于DBCP和C3P0的说明
 * 这两个都是常见的数据库连接池技术
 */
public class DBCPUtil {
	private static DataSource ds;
	static {
		try {
			InputStream in = null;
			Properties properties = new Properties();

			String filepath = "dbcp-config.properties";
			in = DBCPUtil.class.getClassLoader().getResourceAsStream(filepath);
			properties.load(in);//加载相关的配置信息
			ds = BasicDataSourceFactory.createDataSource(properties);
		} catch (Exception e) {//初始化异常
			throw new ExceptionInInitializerError(e);
		}
	}

    /**
     * 得到数据库
     * @return
     */
	public static DataSource getDataSource() {
		return ds;
	}

    /**
     * 获取连接
     * @return
     */
	public static Connection getConnection() {
		try {
			return ds.getConnection();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	//测试
	 /*
	 public static void main(String[] args) throws SQLException {
	 }
	 */

    /**
     * 关闭连接
     * @param connection
     * @param st
     * @param rs
     */
	 public static void release(Connection connection, Statement st, ResultSet rs) {
		 try {
			 if(rs != null) {
				 rs.close();
			 }
		 } catch (SQLException e) {
			 e.printStackTrace();
		 } finally {
			 try {
				 if(st != null) {
					 st.close();
				 }
			 } catch (SQLException e) {
				 e.printStackTrace();
			 } finally {
				 try {
					 if(connection != null) {
						 connection.close();
					 }
				 } catch (SQLException e) {
					 e.printStackTrace();
				 }
			 }
		 }
	 }
	 //name-->setName
	 public static String getSetMethodName(String field) {
		return "set" + field.substring(0, 1).toUpperCase() + field.substring(1);
	 }
}

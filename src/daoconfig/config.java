package daoconfig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;

public class config {
	public static boolean		arg		= true;
	public static DataSource	dataSource;
	public static final String	ORACLE	= "oracleDataSource.properties";
	public static final String	MYSQL	= "DataSource.properties";

	public config(String TYPE) {
		// TODO Auto-generated constructor stub
		if (arg) {
			createdateSource(TYPE);

		}
	}

	// �������ӳ�
	public void createdateSource(String type) {
		// String realPath = this.getClass().getClassLoader().getResource(type)
		// .getFile();
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(config.ORACLE));
			config.dataSource = DruidDataSourceFactory.createDataSource(properties);
			Connection connection = dataSource.getConnection();
			if (connection != null) {
				System.out.println("con success");
				config.arg = false;
			} else {
				System.out.println("con fail");

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

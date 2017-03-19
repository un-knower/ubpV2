package cn.uway.uport.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

/**
 * UPORT数据库连接池管理对象<br>
 * 
 * @author chenrongqiang @ 2013-11-11
 */
public class DbPoolManager {

	/**
	 * 数据库连接会缓存
	 */
	public static Map<String, DataSource> DATASOURCE_CACHE = new HashMap<>();

	/**
	 * 数据库管理器初始化.根据配置文件配置初始化数据源信息
	 * 
	 * @throws Exception
	 */
	public static void initialize() throws Exception {
		// 1、初始化任务表数据源
		initTaskDataSource();
		// 2、初始化安全信息校验数据源
		initSecurityDataSource();
		// 3、初始化节假信息数据源
		initHolidayDataSource();
	}

	/**
	 * 初始化任务表的数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initTaskDataSource() throws Exception {
		DataSource datasource = new BasicDataSource();
		Properties properties = new Properties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.TASK_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.TASK_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.TASK_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.TASK_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.TASK_DATABASE_MAXACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.TASK_DATABASE_MAXIDLE));
		properties.put("maxWait", "60000");
		properties.put("maxWaitMillis", "60000");
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.TASK_DATABASE_URL)));
		// 默认开始testOnReturn和testWhileIdle
		properties.put("testOnReturn", "true");
		properties.put("testWhileIdle", "true");
		datasource = BasicDataSourceFactory.createDataSource(properties);
		DATASOURCE_CACHE.put(ApplicationContext.TASK_DATABASE, datasource);
	}

	/**
	 * 初始化安全校验表的数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initSecurityDataSource() throws Exception {
		boolean enable = Configuration.getBoolean(Configuration.SECURITY_EXTRA_DATABASE_ENABLE);
		// 如果没有启动外部数据库 则默认使用任务表配置的数据源
		if (!enable) {
			DATASOURCE_CACHE.put(ApplicationContext.SECURITY_DATABASE, DATASOURCE_CACHE.get(ApplicationContext.TASK_DATABASE));
			return;
		}
		DataSource datasource = new BasicDataSource();
		Properties properties = new Properties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.SECURITY_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.SECURITY_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.SECURITY_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.SECURITY_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.SECURITY_DATABASE_MAX_ACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.SECURITY_DATABASE_MAX_IDLE));
		properties.put("maxWait", "60000");
		properties.put("maxWaitMillis", "60000");
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.SECURITY_DATABASE_URL)));
		// 默认开始testOnReturn和testWhileIdle
		properties.put("testOnReturn", "true");
		properties.put("testWhileIdle", "true");
		datasource = BasicDataSourceFactory.createDataSource(properties);
		DATASOURCE_CACHE.put(ApplicationContext.SECURITY_DATABASE, datasource);
	}

	/**
	 * 初始化节假日表的数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initHolidayDataSource() throws Exception {
		boolean enable = Configuration.getBoolean(Configuration.HOLIDAY_EXTRA_DATABASE_ENABLE);
		// 如果没有启动外部数据库 则默认使用任务表配置的数据源
		if (!enable) {
			DATASOURCE_CACHE.put(ApplicationContext.HOLIDAY_DATABASE, DATASOURCE_CACHE.get(ApplicationContext.TASK_DATABASE));
			return;
		}
		DataSource datasource = new BasicDataSource();
		Properties properties = new Properties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.HOLIDAY_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.HOLIDAY_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.HOLIDAY_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.HOLIDAY_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.HOLIDAY_DATABASE_MAX_ACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.HOLIDAY_DATABASE_MAX_IDLE));
		properties.put("maxWait", "60000");
		properties.put("maxWaitMillis", "60000");
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.HOLIDAY_DATABASE_URL)));
		// 默认开始testOnReturn和testWhileIdle
		properties.put("testOnReturn", "true");
		properties.put("testWhileIdle", "true");
		datasource = BasicDataSourceFactory.createDataSource(properties);
		DATASOURCE_CACHE.put(ApplicationContext.HOLIDAY_DATABASE, datasource);
	}

	/**
	 * 通过数据库连接串获取驱动类
	 * 
	 * @param url
	 * @return 数据库对应的驱动类型
	 */
	private static String getDriverName(String url) {
		url = url.toLowerCase();
		// 目前只有oracle，暂时只提供oracle的实现(-)
		if (url.indexOf("jdbc:oracle") >= 0)
			return "oracle.jdbc.driver.OracleDriver";
		return null;
	}

	/**
	 * 通过数据库连接串获取数据库校验串
	 * 
	 * @param url
	 * @return 数据库校验串
	 */
	private static String getValidate(String url) {
		url = url.toLowerCase();
		// 目前只有oracle，暂时只提供oracle的实现(-)
		if (url.indexOf("jdbc:oracle") >= 0)
			return "select 1 from dual";
		return null;
	}

	/**
	 * 通过数据库连接池的Key获取数据库连接
	 * 
	 * @param keyName
	 *            连接池KEY
	 * @return 数据库连接对象
	 * @throws SQLException
	 */
	public static Connection getConnection(String keyName) throws SQLException {
		DataSource datasource = DATASOURCE_CACHE.get(keyName);
		if (datasource == null)
			return null;
		return datasource.getConnection();
	}

	/**
	 * 通过安全校验信息数据库连接池的Key获取数据库连接
	 * 
	 * @return 数据库连接对象
	 * @throws SQLException
	 */
	public static Connection getConnectionForSecurity() throws SQLException {
		Connection conn;
		if (Configuration.getBoolean(Configuration.SECURITY_ENABLE)) {
			conn = getConnection(ApplicationContext.SECURITY_DATABASE);
		} else {
			conn = getConnection(ApplicationContext.TASK_DATABASE);
		}
		return conn;
	}

	/**
	 * 通过节假日信息数据库连接池的Key获取数据库连接 如果未配置或未启用节假日数据库，则返回UBP主数据库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForHoliday() throws SQLException {
		Connection conn;
		if (Configuration.getBoolean(Configuration.HOLIDAY_ENABLE)) {
			conn = getConnection(ApplicationContext.HOLIDAY_DATABASE);
		} else {
			conn = getConnection(ApplicationContext.TASK_DATABASE);
		}

		return conn;
	}

	/**
	 * 获取主库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForTask() throws SQLException {
		return getConnection(ApplicationContext.TASK_DATABASE);
	}

}

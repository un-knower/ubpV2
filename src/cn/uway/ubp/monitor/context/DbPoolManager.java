package cn.uway.ubp.monitor.context;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UPORT数据库连接池管理对象<br>
 * 
 * @author chenrongqiang @ 2013-11-11
 */
public class DbPoolManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(DbPoolManager.class);

	/**
	 * 任务表对应的连接池对象key
	 */
	public static final String TASK_DB = "task";

	/**
	 * 告警输出数据库连接池对象key
	 */
	public static final String ALARM_DB = "alarm";

	/**
	 * 忙时信息数据库连接池对象key
	 */
	public static final String BUSY_HOUR_DB = "busy";

	/**
	 * 节假日信息数据库连接池对象key
	 */
	public static final String HOLIDAY_DB = "holiday";
	

	private static Boolean useAlarm = Configuration.getBoolean(Configuration.ALARM_DATABASE_ENABLE);
	private static Boolean useBusyHour = Configuration.getBoolean(Configuration.BUSY_HOUR_EXTRA_DATABASE_ENABLE);
	private static Boolean useHoliday = Configuration.getBoolean(Configuration.HOLIDAY_EXTRA_DATABASE_ENABLE);

	/**
	 * 数据库连接会缓存
	 */
	private static Map<String, DataSource> DS_CACHE = new HashMap<>();

	/**
	 * 数据库管理器初始化.根据配置文件配置初始化数据源信息
	 * 
	 * @throws Exception
	 */
	public static void initialize() throws Exception {
		initTaskDatasource();
		initAlarmExportDatasource();
		initBusyHourDatasource();
		initHolidayDatasource();
	}
	
	/**
	 * 连接池公共配置
	 */
	private static Properties createProperties(){
		Properties properties = new Properties();
		
		// 获取连接等待超时的时间，单位是毫秒
		properties.put("maxWait", "60000");
		// 间隔多久进行一次检测，检测需要关闭的空闲连接，单位是毫秒
		properties.put("timeBetweenEvictionRunsMillis", "60000");
		// 配置一个连接在池中最小生存的时间，单位是毫秒
		properties.put("minEvictableIdleTimeMillis", "300000");
//		// 连接池管理属性
//		properties.put("validationQuery", "SELECT 'x'");//druid
		properties.put("testWhileIdle", "true");
		properties.put("testOnBorrow", "false");
		properties.put("testOnReturn", "false");
//		// 打开PSCache，并且指定每个连接上PSCache的大小
//		properties.put("poolPreparedStatements", "true");
//		properties.put("maxPoolPreparedStatementPerConnectionSize", "20");//druid
//		properties.put("maxOpenPreparedStatements", "20");//dbcp
		
//		// 开发或测试时，很有效
//		properties.put("removeAbandoned", "false");
//		// 空闲多久会被关闭，单位是秒，默认300秒
//		properties.put("removeAbandonedTimeout", "180");
//		properties.put("logAbandoned", "true");
		
		return properties;
	}

	/**
	 * 初始化任务表的数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initTaskDatasource() throws Exception {
		BasicDataSource datasource = new BasicDataSource();
		Properties properties = createProperties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.TASK_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.TASK_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.TASK_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.TASK_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.TASK_DATABASE_MAXACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.TASK_DATABASE_MAXIDLE));
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.TASK_DATABASE_URL)));
		datasource = (BasicDataSource) BasicDataSourceFactory.createDataSource(properties);
		DS_CACHE.put(TASK_DB, datasource);
	}

	/**
	 * 初始化告警输出数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initAlarmExportDatasource() throws Exception {
		// 如果没有启动外部数据库 则默认使用任务表配置的数据源
		if (!useAlarm) {
			DS_CACHE.put(ALARM_DB, DS_CACHE.get(TASK_DB));
			return;
		}
		BasicDataSource datasource = new BasicDataSource();
		Properties properties = createProperties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.ALARM_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.ALARM_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.ALARM_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.ALARM_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.ALARM_DATABASE_MAXACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.ALARM_DATABASE_MAXIDLE));
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.ALARM_DATABASE_URL)));
		datasource = (BasicDataSource)BasicDataSourceFactory.createDataSource(properties);
		DS_CACHE.put(ALARM_DB, datasource);
	}

	/**
	 * 初始化 忙时数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initBusyHourDatasource() throws Exception {
		// 如果没有启动外部数据库 则默认使用任务表配置的数据源
		if (!useBusyHour) {
			DS_CACHE.put(BUSY_HOUR_DB, DS_CACHE.get(TASK_DB));
			return;
		}
		BasicDataSource datasource = new BasicDataSource();
		Properties properties = createProperties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.BUSY_HOUR_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.BUSY_HOUR_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.BUSY_HOUR_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.BUSY_HOUR_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.BUSY_HOUR_DATABASE_MAX_ACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.BUSY_HOUR_DATABASE_MAX_IDLE));
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.BUSY_HOUR_DATABASE_URL)));
		datasource = (BasicDataSource)BasicDataSourceFactory.createDataSource(properties);
		DS_CACHE.put(BUSY_HOUR_DB, datasource);
	}

	/**
	 * 初始化 节假日数据库连接池
	 * 
	 * @throws Exception
	 */
	private static void initHolidayDatasource() throws Exception {
		// 如果没有启动外部数据库 则默认使用任务表配置的数据源
		if (!useHoliday) {
			DS_CACHE.put(HOLIDAY_DB, DS_CACHE.get(TASK_DB));
			return;
		}
		BasicDataSource datasource = new BasicDataSource();
		Properties properties = createProperties();
		properties.put("driverClassName", getDriverName(Configuration.getString(Configuration.HOLIDAY_DATABASE_URL)));
		properties.put("url", Configuration.getString(Configuration.HOLIDAY_DATABASE_URL));
		properties.put("username", Configuration.getString(Configuration.HOLIDAY_DATABASE_USERNAME));
		properties.put("password", Configuration.getString(Configuration.HOLIDAY_DATABASE_PASSWORD));
		properties.put("maxActive", Configuration.getString(Configuration.HOLIDAY_DATABASE_MAX_ACTIVE));
		properties.put("maxIdle", Configuration.getString(Configuration.HOLIDAY_DATABASE_MAX_IDLE));
		properties.put("validationQuery", getValidate(Configuration.getString(Configuration.HOLIDAY_DATABASE_URL)));
		datasource = (BasicDataSource)BasicDataSourceFactory.createDataSource(properties);
		DS_CACHE.put(HOLIDAY_DB, datasource);
	}

	/**
	 * 通过数据库连接串获取驱动类
	 * 
	 * @param url
	 * @return 数据库对应的驱动类型
	 */
	private static String getDriverName(String url) {
		url = url.toLowerCase();
		// 目前只有oracle，暂时只提供oracle的实现
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
		// 目前只有oracle，暂时只提供oracle的实现
		if (url.indexOf("jdbc:oracle") >= 0)
			return "select 1 from dual";
		return null;
	}

	/**
	 * 通过数据库连接池的Key获取数据库连接
	 * 
	 * @param keyName
	 *            连接池KEY UPORT中只有任务表和安全信息表。key只能是task或者security
	 * @return 数据库连接对象
	 * @throws SQLException
	 */
	public static Connection getConnection(String keyName) throws SQLException {
		BasicDataSource datasource = (BasicDataSource) DS_CACHE.get(keyName);
		if (datasource == null)
			return null;
		long start = System.currentTimeMillis();
//		LOGGER.debug("正在获取{}连接,当前活跃连接数{},空闲连接数{}",new Object[]{keyName,datasource.getNumActive(),datasource.getNumIdle()});
		Connection conn = null;
		int tryNum = 0;
		do{
			try{
				conn = datasource.getConnection();
			} catch (Exception e){
				LOGGER.debug("获取{}连接失败,失败次数{},当前活跃连接数{},空闲连接数{},最大连接数{},错误原因:{}",new Object[]{keyName,++tryNum,datasource.getNumActive(),datasource.getNumIdle(),datasource.getMaxActive(),e.getMessage()});
			}
		}while((null == conn)&&(tryNum<3));
		if(null == conn){
			throw new SQLException(String.format("获取%s连接失败,失败次数%s", keyName,tryNum));
		}
		long end = System.currentTimeMillis();
		LOGGER.debug("获取{}连接成功,当前活跃连接数{},空闲连接数{},耗时{}ms",new Object[]{keyName,datasource.getNumActive(),datasource.getNumIdle(),(end-start)});
		return conn;
	}

	/**
	 * 获取主库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForTask() throws SQLException {
		return getConnection(TASK_DB);
	}

	/**
	 * 获取告警输出数据库连接 未启用告警输出单独数据库配置时，返回主库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForAlarm() throws SQLException {
		return getConnection(ALARM_DB);
	}

	/**
	 * 获取忙时数据库连接 未启用忙时表单独数据库配置时，返回主库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForBusyHour() throws SQLException {
		return getConnection(BUSY_HOUR_DB);
	}

	/**
	 * 获取节假日数据库连接 未启用节假日表单独数据库配置时，返回主库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnectionForHoliday() throws SQLException {
		return getConnection(HOLIDAY_DB);
	}

}

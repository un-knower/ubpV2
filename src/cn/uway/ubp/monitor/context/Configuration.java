package cn.uway.ubp.monitor.context;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.system.configuration.BaseConfiguration;
import cn.uway.framework.system.configuration.PropertiesXML;

/**
 * UPORT配置文件缓存类<br>
 * 
 * @author chenrongqiang @ 2013-11-6
 */
public class Configuration extends BaseConfiguration {

	/**
	 * 应用程序根目录，默认为当前
	 */
	public static String ROOT_DIRECTORY = ".";

	/**
	 * UPORT配置文件路径
	 */
	private static final String CONFIG_FILE = "conf/monitor.xml";

	/**
	 * 任务加载运算调试模式开关
	 */
	public static final String TASK_ENABLE_DEBUG = "monitor.task.enable-debug";

	/**
	 * 任务加载的周期
	 */
	public static final String TASK_LOAD_PERIOD = "monitor.task.load-period";

	/**
	 * 任务加载线程最大并发数
	 */
	public static final String TASK_QUEUE_CAPACITY = "monitor.task.queue-capacity";

	/**
	 * 任务加载线程最大并发数
	 */
	public static final String TASK_MAX_THREADS = "monitor.task.max-threads";

	/**
	 * UBP任务表所在数据库连接串
	 */
	public static final String TASK_DATABASE_URL = "monitor.task.task-database.url";

	/**
	 * UBP任务表所在数据库用户名
	 */
	public static final String TASK_DATABASE_USERNAME = "monitor.task.task-database.username";

	/**
	 * UBP任务表所在数据库密码
	 */
	public static final String TASK_DATABASE_PASSWORD = "monitor.task.task-database.password";

	/**
	 * UBP任务表所在数据库最大连接数
	 */
	public static final String TASK_DATABASE_MAXACTIVE = "monitor.task.task-database.max-active";

	/**
	 * UBP任务表所在数据库最大空闲连接数
	 */
	public static final String TASK_DATABASE_MAXIDLE = "monitor.task.task-database.max-idle";

	/**
	 * 数据源加载调试模式开关
	 */
	public static final String DATASOURCE_ENABLE_DEBUG = "monitor.datasource.enable-debug";

	/**
	 * 数据源加载频率
	 */
	public static final String DATASOURCE_LOAD_PERIOD = "monitor.datasource.datasource-loader.period";

	/**
	 * 数据源加载线程池大小
	 */
	public static final String DATASOURCE_LOAD_MAX_THREADS = "monitor.datasource.datasource-loader.max-threads";

	/**
	 * 数据源加载队列最大容量 TODO 未使用
	 */
	public static final String DATASOURCE_LOAD_CAPACITY = "monitor.datasource.datasource-loader.queue-capacity";

	/**
	 * 单个数据源每次最长加载时间 单位：分钟
	 */
	public static final String DATASOURCE_MAX_LOAD_TIME = "monitor.datasource.datasource-loader.max-load-time";

	/**
	 * 数据源加载容错比例
	 */
	public static final String DATASOURCE_FAULT_TOLERANCE_PERCENT = "monitor.datasource.datasource-loader.fault-tolerance-percent";

	/**
	 * 数据源容错检验小时数.
	 * 
	 * <pre>
	 * 	temporary change:sg
	 * 	date:2014-4-29
	 * 	explain: 数据源加载后，如果与sysdate已经相差一个周期+N个小时后，波动仍然超限，此时不再限制
	 * </pre>
	 */
	public static final String DATASOURCE_FAULT_TOLERANCE_PERCENT_CHECK_HOURS = "monitor.datasource.datasource-loader.fault-tolerance-percent-check-hours";

	/**
	 * 非日志驱动类数据源无数据时加载的最大超时时间（超时后跳过该时间点），负数即为不启用超时时间（永不超时）
	 */
	public static final String DATASOURCE_LOAD_TIMEOUT = "monitor.datasource.datasource-loader.load-timeout";

	/**
	 * 数据文件根目录
	 */
	public static final String DATASOURCE_FILE_ROOT_DIR = "monitor.datasource.datasource-loader.file-root-dir";

	/**
	 * 数据文件字符集
	 */
	public static final String DATASOURCE_FILE_CHARSET = "monitor.datasource.datasource-loader.file-charset";

	/**
	 * 数据源清理频率
	 */
	public static final String DATASOURCE_DEPOT_PERIOD = "monitor.datasource.datasource-depot.period";

	/**
	 * 数据源清理的滑动时间窗口 即在最大任务分析周期的基础上滑动的时间 单位：小时
	 */
	public static final String DATASOURCE_SLIDE_HOUR = "monitor.datasource.datasource-depot.slide-hours";

	/**
	 * 数据文件存储时间(数据文件生成时间)，即数据源文件生成时间离当前时间的间隔 单位：小时
	 */
	public static final String DATASOURCE_LIVE_HOUR = "monitor.datasource.datasource-depot.live-hours";

	/**
	 * 告警输出Alarm_Text字段字符集
	 */
	public static final String ALARM_CHARSET = "monitor.alarm-export.charset";

	/**
	 * 告警输出数据库开关 如enable配置为false。则默认启用任务表的数据库
	 */
	public static final String ALARM_DATABASE_ENABLE = "monitor.alarm-export.enable-extra-database";

	/**
	 * 告警明细入库启用（联通不需要设置为false,节省性能）
	 */
	public static final String ALARM_DETAIL_ENABLE = "monitor.alarm-export.enable-alarm-detail";

	/**
	 * 告警输出数据库连接串
	 */
	public static final String ALARM_DATABASE_URL = "monitor.alarm-export.database.url";

	/**
	 * 告警输出数据库用户名
	 */
	public static final String ALARM_DATABASE_USERNAME = "monitor.alarm-export.database.username";

	/**
	 * 告警输出数据库密码
	 */
	public static final String ALARM_DATABASE_PASSWORD = "monitor.alarm-export.database.password";

	/**
	 * 告警输出数据库最大连接数
	 */
	public static final String ALARM_DATABASE_MAXACTIVE = "monitor.alarm-export.database.max-active";

	/**
	 * 告警输出数据库最大空闲连接数
	 */
	public static final String ALARM_DATABASE_MAXIDLE = "monitor.alarm-export.database.max-idle";

	/**
	 * 最大可以使用的内存比例
	 */
	public static final String MAX_MEMEROY_USE = "monitor.memory-controll.max-percent";

	/**
	 * 内存回收频率
	 */
	public static final String MEMEROY_GC_PERIOD = "monitor.memory-controll.gc-period";

	/**
	 * 忙时功能开关
	 */
	public static final String BUSY_HOUR_ENABLE = "monitor.busy-hour.enable";

	/**
	 * 忙时信息刷新频率
	 */
	public static final String BUSY_HOUR_REFRESH_PERIOD = "monitor.busy-hour.refresh-period";

	/**
	 * 忙时信息保存天数
	 */
	public static final String BUSY_HOUR_CACHE_DAYS = "monitor.busy-hour.cache-days";

	/**
	 * 忙时加载数据库是否启动开关 true表示打开，false表示关闭
	 */
	public static final String BUSY_HOUR_EXTRA_DATABASE_ENABLE = "monitor.busy-hour.load-database.enable";

	/**
	 * 忙时加载数据库连接串
	 */
	public static final String BUSY_HOUR_DATABASE_URL = "monitor.busy-hour.load-database.database.url";

	/**
	 * 忙时加载数据库用户名
	 */
	public static final String BUSY_HOUR_DATABASE_USERNAME = "monitor.busy-hour.load-database.database.username";

	/**
	 * 忙时加载数据库密码
	 */
	public static final String BUSY_HOUR_DATABASE_PASSWORD = "monitor.busy-hour.load-database.database.password";

	/**
	 * 忙时加载数据库最大连接数
	 */
	public static final String BUSY_HOUR_DATABASE_MAX_ACTIVE = "monitor.busy-hour.load-database.database.max-active";

	/**
	 * 忙时加载数据库最大空闲连接数
	 */
	public static final String BUSY_HOUR_DATABASE_MAX_IDLE = "monitor.busy-hour.load-database.database.max-idle";

	/**
	 * 节假日功能开关
	 */
	public static final String HOLIDAY_ENABLE = "monitor.holiday.enable";

	/**
	 * 节假日信息刷新频率
	 */
	public static final String HOLIDAY_REFRESH_PERIOD = "monitor.holiday.refresh-period";

	/**
	 * 节假日加载数据库是否启动开关 true表示打开，false表示关闭
	 */
	public static final String HOLIDAY_EXTRA_DATABASE_ENABLE = "monitor.holiday.load-database.enable";

	/**
	 * 节假日加载数据库连接串
	 */
	public static final String HOLIDAY_DATABASE_URL = "monitor.holiday.load-database.database.url";

	/**
	 * 节假日加载数据库用户名
	 */
	public static final String HOLIDAY_DATABASE_USERNAME = "monitor.holiday.load-database.database.username";

	/**
	 * 节假日加载数据库密码
	 */
	public static final String HOLIDAY_DATABASE_PASSWORD = "monitor.holiday.load-database.database.password";

	/**
	 * 节假日加载数据库最大连接数
	 */
	public static final String HOLIDAY_DATABASE_MAX_ACTIVE = "monitor.holiday.load-database.database.max-active";

	/**
	 * 节假日加载数据库最大空闲连接数
	 */
	public static final String HOLIDAY_DATABASE_MAX_IDLE = "monitor.holiday.load-database.database.max-idle";

	/**
	 * UBP配置文件初始化方法
	 * 
	 * @param dir
	 *            应用程序根目录
	 * @throws Exception
	 */
	public static void initialize(String dir) throws Exception {
		if (StringUtils.isNotBlank(dir))
			ROOT_DIRECTORY = dir;

		FileInputStream fis = null;
		try {
			File file = null;
			if ("".equals(ROOT_DIRECTORY)) {
				file = new File(CONFIG_FILE);
			} else {
				file = new File(ROOT_DIRECTORY, CONFIG_FILE);
			}

			fis = new FileInputStream(file);
			PropertiesXML properties = new PropertiesXML(fis);
			// 读取任务相关配置
			getTaskInfo(properties);
			// 数据源相关配置读取
			getDatasourceInfo(properties);
			// 告警输出相关配置读取
			getAlarmExportInfo(properties);
			// 内存控制参数读取
			readProperty(properties, MAX_MEMEROY_USE);
			readProperty(properties, MEMEROY_GC_PERIOD);
			// 忙时配置读取
			getBusyHourInfo(properties);
			// 节假日配置读取
			getHolidayInfo(properties);

			fis.close();
		} finally {
			IOUtils.closeQuietly(fis);
		}
	}

	/**
	 * 任务表相关的配置，任务加载周期、数据库信息
	 * 
	 * @param properties
	 */
	private static void getTaskInfo(PropertiesXML properties) {
		readProperty(properties, TASK_ENABLE_DEBUG);
		readProperty(properties, TASK_LOAD_PERIOD);
		readProperty(properties, TASK_QUEUE_CAPACITY);
		readProperty(properties, TASK_MAX_THREADS);
		readProperty(properties, TASK_DATABASE_URL);
		readProperty(properties, TASK_DATABASE_USERNAME);
		readProperty(properties, TASK_DATABASE_PASSWORD);
		readProperty(properties, TASK_DATABASE_MAXACTIVE);
		readProperty(properties, TASK_DATABASE_MAXIDLE);
	}

	/**
	 * 数据源相关配置，包含数据源加载、清理机制配置
	 * 
	 * @param properties
	 */
	private static void getDatasourceInfo(PropertiesXML properties) {
		readProperty(properties, DATASOURCE_ENABLE_DEBUG);
		readProperty(properties, DATASOURCE_LOAD_PERIOD);
		readProperty(properties, DATASOURCE_LOAD_MAX_THREADS);
		readProperty(properties, DATASOURCE_LOAD_CAPACITY);
		readProperty(properties, DATASOURCE_MAX_LOAD_TIME);
		readProperty(properties, DATASOURCE_FAULT_TOLERANCE_PERCENT);

		/**
		 * <pre>
		 * 	temporary change:sg
		 * 	date:2014-4-29
		 * 	explain: 默认8小时，避免升级时要修改ini影响
		 * </pre>
		 */
		readProperty(properties,
				DATASOURCE_FAULT_TOLERANCE_PERCENT_CHECK_HOURS, "8");
		readProperty(properties, DATASOURCE_LOAD_TIMEOUT);
		readProperty(properties, DATASOURCE_FILE_ROOT_DIR, "");
		readProperty(properties, DATASOURCE_FILE_CHARSET, "UTF-8");
		readProperty(properties, DATASOURCE_DEPOT_PERIOD);
		readProperty(properties, DATASOURCE_SLIDE_HOUR);
		readProperty(properties, DATASOURCE_LIVE_HOUR);
	}

	/**
	 * 读取告警输出配置
	 * 
	 * @param properties
	 */
	private static void getAlarmExportInfo(PropertiesXML properties) {
		readBooleanProperty(properties, ALARM_DETAIL_ENABLE);
		readProperty(properties, ALARM_CHARSET, "UTF-8");

		readBooleanProperty(properties, ALARM_DATABASE_ENABLE);
		boolean enable = getBoolean(ALARM_DATABASE_ENABLE);
		if (!enable)
			return;

		readProperty(properties, ALARM_DATABASE_URL);
		readProperty(properties, ALARM_DATABASE_USERNAME);
		readProperty(properties, ALARM_DATABASE_PASSWORD);
		readProperty(properties, ALARM_DATABASE_MAXACTIVE);
		readProperty(properties, ALARM_DATABASE_MAXIDLE);
	}

	/**
	 * 读取忙时相关配置
	 * 
	 * @param properties
	 */
	private static void getBusyHourInfo(PropertiesXML properties) {
		readBooleanProperty(properties, BUSY_HOUR_ENABLE);
		boolean enable = getBoolean(BUSY_HOUR_ENABLE);
		if (!enable)
			return;

		readProperty(properties, BUSY_HOUR_REFRESH_PERIOD);
		readProperty(properties, BUSY_HOUR_CACHE_DAYS);
		readBooleanProperty(properties, BUSY_HOUR_EXTRA_DATABASE_ENABLE);
		enable = getBoolean(BUSY_HOUR_ENABLE);
		if (!enable)
			return;

		readProperty(properties, BUSY_HOUR_DATABASE_URL);
		readProperty(properties, BUSY_HOUR_DATABASE_USERNAME);
		readProperty(properties, BUSY_HOUR_DATABASE_PASSWORD);
		readProperty(properties, BUSY_HOUR_DATABASE_MAX_ACTIVE);
		readProperty(properties, BUSY_HOUR_DATABASE_MAX_IDLE);
	}

	/**
	 * 读取节假日配置信息
	 * 
	 * @param properties
	 */
	private static void getHolidayInfo(PropertiesXML properties) {
		readBooleanProperty(properties, HOLIDAY_ENABLE);
		boolean enable = getBoolean(HOLIDAY_ENABLE);
		if (!enable)
			return;

		readProperty(properties, HOLIDAY_REFRESH_PERIOD);

		readBooleanProperty(properties, HOLIDAY_EXTRA_DATABASE_ENABLE);
		enable = getBoolean(HOLIDAY_EXTRA_DATABASE_ENABLE);
		if (!enable)
			return;

		readProperty(properties, HOLIDAY_DATABASE_URL);
		readProperty(properties, HOLIDAY_DATABASE_USERNAME);
		readProperty(properties, HOLIDAY_DATABASE_PASSWORD);
		readProperty(properties, HOLIDAY_DATABASE_MAX_ACTIVE);
		readProperty(properties, HOLIDAY_DATABASE_MAX_IDLE);
	}

}

package cn.uway.uport.context;

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
	 * 应用程序根目录，默认为空，即当前
	 */
	public static String ROOT_DIRECTORY = ".";

	/**
	 * UPORT配置文件路径
	 */
	public static final String CONFIG_FILE = "conf/uport.xml";

	/**
	 * HTTP端口配置
	 */
	public static final String PORT = "uport.http-port";
	
	/**
	 * 修改任务时，是否校准任务运行时间（校准即取任务在库中的时间与当前修改任务时间比较，取较大的）
	 */
	public static final String CALIBRATION_CURR_MONITOR_TIME = "uport.calibration-curr-monitor-time";

	/**
	 * UBP任务表数据库连接URL
	 */
	public static final String TASK_DATABASE_URL = "uport.task-database.url";

	/**
	 * UBP任务表数据库连接用户名
	 */
	public static final String TASK_DATABASE_USERNAME = "uport.task-database.username";

	/**
	 * UBP任务表数据库连接密码
	 */
	public static final String TASK_DATABASE_PASSWORD = "uport.task-database.password";

	/**
	 * UBP任务表数据库连接最大连接数
	 */
	public static final String TASK_DATABASE_MAXACTIVE = "uport.task-database.max-active";

	/**
	 * UBP任务表数据库连接最大空闲连接数
	 */
	public static final String TASK_DATABASE_MAXIDLE = "uport.task-database.max-idle";

	/**
	 * 是否开启UPORT安全校验功能
	 */
	public static final String SECURITY_ENABLE = "uport.security.enable";

	/**
	 * UBP程序在UWAY_PROGRAM_DEFINE表定义的ID
	 */
	public static final String SECURITY_PROGRAM_ID = "uport.security.program-id";

	/**
	 * 程序定义信息缓存刷新频率 单位：分钟
	 */
	public static final String SECURITY_PROGRAM_DEFINE_REFRESH_PERIOD = "uport.security.program-define-refresh-period";

	/**
	 * 安全校验信息刷新频率 单位：分钟
	 */
	public static final String SECURITY_INFO_REFRESH_PERIOD = "uport.security.security-info-refresh-period";

	/**
	 * 程序定义和安全信息是否从其他数据库加载，如enable为false，则默认读取任务表的配置
	 */
	public static final String SECURITY_EXTRA_DATABASE_ENABLE = "uport.security.load-database.enable";

	/**
	 * 安全信息加载数据库url地址
	 */
	public static final String SECURITY_DATABASE_URL = "uport.security.load-database.database.url";

	/**
	 * 安全信息加载数据库用户名
	 */
	public static final String SECURITY_DATABASE_USERNAME = "uport.security.load-database.database.username";

	/**
	 * 安全信息加载数据库密码
	 */
	public static final String SECURITY_DATABASE_PASSWORD = "uport.security.load-database.database.password";

	/**
	 * 安全信息加载数据库最大连接数
	 */
	public static final String SECURITY_DATABASE_MAX_ACTIVE = "uport.security.load-database.database.max-active";

	/**
	 * 安全信息加载数据库最多空闲连接数
	 */
	public static final String SECURITY_DATABASE_MAX_IDLE = "uport.security.load-database.database.max-idle";

	/**
	 * 节假日功能开关
	 */
	public static final String HOLIDAY_ENABLE = "uport.holiday.enable";

	/**
	 * 节假日信息刷新频率
	 */
	public static final String HOLIDAY_REFRESH_PERIOD = "uport.holiday.refresh-period";

	/**
	 * 节假日加载数据库是否启动开关 true表示打开，false表示关闭
	 */
	public static final String HOLIDAY_EXTRA_DATABASE_ENABLE = "uport.holiday.load-database.enable";

	/**
	 * 节假日加载数据库连接串
	 */
	public static final String HOLIDAY_DATABASE_URL = "uport.holiday.load-database.database.url";

	/**
	 * 节假日加载数据库用户名
	 */
	public static final String HOLIDAY_DATABASE_USERNAME = "uport.holiday.load-database.database.username";

	/**
	 * 节假日加载数据库密码
	 */
	public static final String HOLIDAY_DATABASE_PASSWORD = "uport.holiday.load-database.database.password";

	/**
	 * 节假日加载数据库最大连接数
	 */
	public static final String HOLIDAY_DATABASE_MAX_ACTIVE = "uport.holiday.load-database.database.max-active";

	/**
	 * 节假日加载数据库最大空闲连接数
	 */
	public static final String HOLIDAY_DATABASE_MAX_IDLE = "uport.holiday.load-database.database.max-idle";

	/**
	 * 接口访问控制开关 如enable为false，则不控制接口访问IP和频率
	 */
	public static final String ACCESS_CONTROL_ENABLE = "uport.access-control.enable";

	/**
	 * 接口访问控制时间范围 ,单位：秒;比如控制10秒内的访问次数
	 */
	public static final String ACCESS_CONTROL_UNIT_SECONDS = "uport.access-control.control-unit-seconds";

	/**
	 * 在ACCESS_CONTROL_TIME_RANGE的访问次数限制
	 */
	public static final String ACCESS_CONTROL_ACCESS_COUNT = "uport.access-control.access-count";

	/**
	 * 在超出访问频率后限制访问的时间 单位：秒
	 */
	public static final String ACCESS_CONTROL_FORBIDDEN_TIME = "uport.access-control.forbidden-time";

	/**
	 * 允许访问的IP列表，在此范围外的IP，访问将被直接拒绝
	 */
	public static final String ACCESS_CONTROL_ALLOW_IP_LIST = "uport.access-control.allow-ip-list";

	/**
	 * UBP配置文件初始化方法
	 * 
	 * @param dir 应用程序根目录
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
			readProperty(properties, PORT);
			readBooleanProperty(properties, CALIBRATION_CURR_MONITOR_TIME);
			readProperty(properties, TASK_DATABASE_URL);
			readProperty(properties, TASK_DATABASE_USERNAME);
			readProperty(properties, TASK_DATABASE_PASSWORD);
			readProperty(properties, TASK_DATABASE_MAXACTIVE);
			readProperty(properties, TASK_DATABASE_MAXIDLE);
			readBooleanProperty(properties, SECURITY_ENABLE);
			getSecurityInfo(properties);
			getHolidayInfo(properties);
			readBooleanProperty(properties, ACCESS_CONTROL_ENABLE);
			Boolean enable = getBoolean(ACCESS_CONTROL_ENABLE);
			if (enable) {
				readProperty(properties, ACCESS_CONTROL_UNIT_SECONDS);
				readProperty(properties, ACCESS_CONTROL_FORBIDDEN_TIME);
				readProperty(properties, ACCESS_CONTROL_ACCESS_COUNT);
				readProperty(properties, ACCESS_CONTROL_ALLOW_IP_LIST);
			}
		} finally {
			IOUtils.closeQuietly(fis);
		}
	}

	/**
	 * 读取安全校验信息
	 * 
	 * @param properties
	 */
	private static void getSecurityInfo(PropertiesXML properties) {
		Boolean enable = getBoolean(SECURITY_ENABLE);
		// 如果没有打开开关 直接不读取子配置项
		if (!enable)
			return;
		
		readProperty(properties, SECURITY_PROGRAM_ID);
		readProperty(properties, SECURITY_PROGRAM_DEFINE_REFRESH_PERIOD);
		readProperty(properties, SECURITY_INFO_REFRESH_PERIOD);
		readBooleanProperty(properties, SECURITY_EXTRA_DATABASE_ENABLE);
		enable = getBoolean(SECURITY_EXTRA_DATABASE_ENABLE);
		if (enable) {
			// 如果开关打开,则读取配置文件中的配置
			readProperty(properties, SECURITY_DATABASE_URL);
			readProperty(properties, SECURITY_DATABASE_USERNAME);
			readProperty(properties, SECURITY_DATABASE_PASSWORD);
			readProperty(properties, SECURITY_DATABASE_MAX_ACTIVE);
			readProperty(properties, SECURITY_DATABASE_MAX_IDLE);
		}
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

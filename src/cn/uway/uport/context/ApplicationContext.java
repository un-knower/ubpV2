package cn.uway.uport.context;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.uport.context.task.HolidayDataLoadTask;

/**
 * <pre>
 * UPORT程序上下文管理器
 * 1、负责在程序运行之前加载配置文件
 * 2、控制系统内部生效模块的启动
 * 
 * @author chenrongqiang @ 2013-11-11
 * </pre>
 */
public class ApplicationContext {

	/**
	 * UBP任务表数据库连接池key
	 */
	public static final String TASK_DATABASE = "task";

	/**
	 * UBP安全校验和程序定义信息数据库连接池的key
	 */
	public static final String SECURITY_DATABASE = "security";

	/**
	 * 节假日数据库连接池key
	 */
	public static final String HOLIDAY_DATABASE = "holiday";

	/**
	 * 节假日调度服务
	 */
	private static ScheduledExecutorService scheduleExecutor;
	
	private static final Logger logger = LoggerFactory.getLogger(ApplicationContext.class);

	/**
	 * <pre>
	 * 程序上下文初始化
	 * 1、初始化配置文件
	 * 2、初始化UPORT在运行过程中需要用到的数据库连接信息
	 * 3、根据配置开关初始化安全信息模块
	 * 4、根据配置开关初始化访问控制模块
	 * 
	 * @param dir 应用程序根目录
	 * </pre>
	 */
	public static void initialize(String dir) throws Exception {
		// 1、初始化配置文件 如果配置文件初始化失败，程序无法启动
		Configuration.initialize(dir);
		
		// 2、数据库连接池初始化
		DbPoolManager.initialize();
		
		// 3、安全校验模块初始化
		SecurityValidator.initialize();
		ProgramDefineValidator.initialize();
		
		// 4、IP访问控制模块初始化
		AccessControl.initialize();

		// 5、加载节假日数据，设置调度
		boolean enable = Configuration.getBoolean(Configuration.HOLIDAY_ENABLE);
		if (enable) {
			initHolidayChecker();
		} else {
			logger.info("节假日未启用");
		}
	}
	
	/**
	 * 加载节假日数据，设置调试
	 */
	private static void initHolidayChecker() {
		scheduleExecutor = Executors.newSingleThreadScheduledExecutor();

		// 暂定:以程序启动时间为基准,当前时间加载数据,之后间隔holidayRefreshPeriod分钟重新加载一次
		long holidayRefreshPeriod = Configuration.getLong(Configuration.HOLIDAY_REFRESH_PERIOD);
		scheduleExecutor.scheduleAtFixedRate(new HolidayDataLoadTask(), 0L, holidayRefreshPeriod, TimeUnit.MINUTES);
	}

}

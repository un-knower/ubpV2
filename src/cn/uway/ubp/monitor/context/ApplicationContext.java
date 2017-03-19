package cn.uway.ubp.monitor.context;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.context.task.HolidayDataLoadTask;

/**
 * UBP程序上下文<br>
 * 1、负责管理配置文件的读取<br>
 * 2、UBP中数据库连接池对象的创建<br>
 * 3、忙时、节假日信息缓存模块加载管理<br>
 * 
 * @author chenrongqiang @ 2013-11-11
 */
public class ApplicationContext {

	/**
	 * 节假日调度服务
	 */
	private static ScheduledExecutorService scheduleExecutor;
	
	private static final Logger logger = LoggerFactory.getLogger(ApplicationContext.class);

	/**
	 * 初始化方法
	 * 
	 * @param dir 应用程序根目录
	 */
	public static void initialize(String dir) throws Exception {
		// 1、负责管理配置文件的读取
		Configuration.initialize(dir);
		
		// 2、创建数据库连接池对象
		DbPoolManager.initialize();
		
		// 3、启动忙时管理
		BusyHourManager.initialize();
		
		// 4、加载节假日数据，设置调度
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
		long holidayRefreshPeriod = Configuration.getInteger(Configuration.HOLIDAY_REFRESH_PERIOD);
		scheduleExecutor.scheduleAtFixedRate(new HolidayDataLoadTask(), 0L, holidayRefreshPeriod, TimeUnit.MINUTES);
	}

}

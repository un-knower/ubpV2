package cn.uway.ubp.monitor;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.context.ApplicationContext;
import cn.uway.ubp.monitor.data.DataSourceLoader;
import cn.uway.ubp.monitor.gc.CollectionDepot;
import cn.uway.ubp.monitor.task.JobGroup;
import cn.uway.ubp.monitor.task.JobGroupQueue;
import cn.uway.ubp.monitor.task.MonitorTaskTrigger;
import cn.uway.ubp.monitor.task.TimerTaskLoader;

/**
 * UBP监控程序启动类<br>
 * 
 * @author chenrongqiang 2013-6-5
 */
public class MonitorRunner {

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(MonitorRunner.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 应用程序根目录
		String dir = null;
		if (args != null && args.length == 1)
			dir = args[0];

		// UBP程序上下文初始化
		ApplicationContext.initialize(dir);

		// 启动数据源加载模块
		new DataSourceLoader().startup();

		// 启动任务调度模块
		startTrigger();

		// 启动垃圾回收模块
		new CollectionDepot().start();
		
		logger.info("UBP-Monitor启动完成");
	}

	private static void startTrigger() {
		// 任务组队列
		JobGroupQueue<JobGroup> jobGroupQueue = new JobGroupQueue<JobGroup>();
		// 当前运行任务表
		ConcurrentHashMap<String, JobGroup> runingJobMap = new ConcurrentHashMap<String, JobGroup>();

		TimerTaskLoader timerTaskLoader = new TimerTaskLoader(jobGroupQueue, runingJobMap);
		timerTaskLoader.start();
		MonitorTaskTrigger monitorTaskTrigger = new MonitorTaskTrigger(jobGroupQueue, runingJobMap);
		monitorTaskTrigger.start();
	}

}

package cn.uway.ubp.monitor.task;

import java.util.Date;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.context.Configuration;

/**
 * 监控任务加载线程
 * 
 * @author chenrongqiang @ 2013-8-5
 */
public class TimerTaskLoader extends Thread {

	/**
	 * 定时调度器
	 */
	private Timer timer = new Timer();

	/**
	 * 定时任务执行周期 单位分钟
	 */
	private int period;

	/**
	 * 分组任务队列
	 */
	private JobGroupQueue<JobGroup> jobGroupQueue;

	private ConcurrentHashMap<String, JobGroup> runingJobMap;

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(TimerTaskLoader.class);

	/**
	 * <pre>
	 * 构造方法
	 * 
	 * @param jobGroupQueue 支持分组的任务队列
	 * @param runingTaskQueque 正在运行的任务
	 * </pre>
	 */
	public TimerTaskLoader(JobGroupQueue<JobGroup> jobGroupQueue, ConcurrentHashMap<String, JobGroup> runingJobMap) {
		super("监控任务加载调度线程");
		this.period = (Configuration.getInteger(Configuration.TASK_LOAD_PERIOD) <= 0 ? 5 : Configuration.getInteger(Configuration.TASK_LOAD_PERIOD));
		this.jobGroupQueue = jobGroupQueue;
		this.runingJobMap = runingJobMap;
	}

	@Override
	public void run() {
		timer.schedule(new MonitorTaskLoader(jobGroupQueue, runingJobMap), new Date(), period * 60 * 1000L);
		logger.debug("监控任务加载调度启动，扫描频率{}分钟", period);
	}

}

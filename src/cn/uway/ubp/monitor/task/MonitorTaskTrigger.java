package cn.uway.ubp.monitor.task;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.indicator.job.JobFuture;

/**
 * <pre>
 * 监控任务运算调度线程
 * 1、从任务队列中提取需要进行指标运算的任务
 * 2、通过指标、规则等信息判断任务本次是否需要执行指标运算
 * 3、提交指标运算job任务并且处理返回结果
 * 【注：指标和规则运算的判断逻辑独立.指标运算在job中同步执行】
 * 
 * @author chenrongqiang @ 2013-8-5
 * </pre>
 */
public class MonitorTaskTrigger extends Thread {

	// 监控任务队列
	private JobGroupQueue<JobGroup> jobGroupQueue;

	// 正在运行的监控任务 K-任务ID V-监控任务组
	private ConcurrentHashMap<String, JobGroup> runningJobMap;

	// 触发器控制开关 用于控制任务调度和触发
	protected volatile boolean trrigerFlag = true;

	// Job执行线程池
	private CompletionService<JobFuture> expressionJobPool;

	private ExecutorService executorService;

	// 日志打印
	private static final Logger logger = LoggerFactory.getLogger(MonitorTaskTrigger.class);

	public MonitorTaskTrigger(JobGroupQueue<JobGroup> jobGroupQueue, ConcurrentHashMap<String, JobGroup> runingJobMap) {
		super("监控任务运算调度线程");

		this.jobGroupQueue = jobGroupQueue;
		this.runningJobMap = runingJobMap;
		// 任务加载线程最大并发数
		int threadPoolSize = Configuration.getInteger(Configuration.TASK_MAX_THREADS) <= 0 ? 10 : Configuration.getInteger(Configuration.TASK_MAX_THREADS);
		// 最多可以同时运行的任务数=线程池数量
		executorService = Executors.newFixedThreadPool(threadPoolSize);
		expressionJobPool = new ExecutorCompletionService<JobFuture>(executorService);
		// 启动提取线程
		new TakeThread().start();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("监控任务触发器线程");

		// 如果触发器开关打开 则开始进行任务调度
		while (trrigerFlag) {
			try {
				JobGroup jobGroup = jobGroupQueue.take();

				/*
				 * 加载任务已经保证相同的任务不会两次被加载，此去要加上反而出错 String monitorTaskId =
				 * jobGroup.getMonitorTaskId(); boolean isRunning =
				 * runningJobMap.containsKey(monitorTaskId); // 如果监控任务已经在运行中
				 * 则不再执行 if (isRunning) { LOGGER.debug("监控任务{}正在运行中",
				 * monitorTaskId); continue; }
				 * 
				 * runningJobMap.put(monitorTaskId, jobGroup);
				 */
				expressionJobPool.submit(jobGroup);
			} catch (InterruptedException e) {
				logger.error("从任务队列中提取监控任务异常", e);
				continue;
			}
		}
	}

	/**
	 * 监控任务停止方法<br>
	 * 将在本次运行任务提交后结束调度
	 */
	public void stopTrigger() {
		this.trrigerFlag = false;
	}

	/**
	 * 指标表达式运算线程池提取线程
	 * 
	 * @author chenrongqiang @ 2013-8-6
	 */
	class TakeThread extends Thread {

		public TakeThread() {
			this.setName("监控任务触发器提取线程");
		}

		// 提取线程执行方法
		public void run() {
			while (true) {
				int code;
				String monitorTaskId = null;
				long timeConsuming;
				try {
					Future<JobFuture> future = expressionJobPool.take();

					JobFuture jobFuture = future.get();
					code = jobFuture.getCode();
					monitorTaskId = jobFuture.getMonitorTaskId();
					timeConsuming = jobFuture.getTimeConsuming();
					
					switch (code) {
						case 1 :
							logger.debug("监控任务{}运算完成，耗时{}ms", monitorTaskId, timeConsuming);
							break;
						default :
							logger.error("监控任务{}运行失败，耗时{}ms", monitorTaskId, timeConsuming);
					}
				} catch (InterruptedException e) {
					logger.error("线程被中断", e);
				} catch (ExecutionException e) {
					logger.error("线程执行异常", e);
				} catch (Exception e) {
					logger.error("线程处理异常", e);
				} finally {
					if (monitorTaskId != null) {
						runningJobMap.remove(monitorTaskId);
						logger.debug("监控任务{}移除", monitorTaskId);
					}
				}
			}
		}
	}

}

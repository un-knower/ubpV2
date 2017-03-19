package cn.uway.ubp.monitor.task;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.MonitorTaskStatusDAO;
import cn.uway.ubp.monitor.indicator.filter.FilterBuilder;
import cn.uway.ubp.monitor.indicator.job.JobFuture;

/**
 * <pre>
 * 监控任务组
 * 本类包含了同属于一个监控任务所有子任务(即monitor_task_id相同)
 * 功能:
 * 	1,保存监控任务中所有子任务
 * 	2,控制各子任务运行先后顺序
 * @author Chris @ 2013-09-09
 * </pre>
 */
public class JobGroup implements Callable<JobFuture> {

	/**
	 * 监控任务ID
	 */
	private String monitorTaskId;

	/**
	 * 需要执行的监控任务JOB列表
	 */
	private List<Job> jobList;

	/**
	 * 运算开始时间
	 */
	private long startTime;

	/**
	 * 运算结束时间
	 */
	private long endTime;

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(JobGroup.class);

	public JobGroup(String monitorTaskId) {
		this.monitorTaskId = monitorTaskId;
		jobList = new ArrayList<>();
	}

	@Override
	public JobFuture call() throws Exception {
		startTime = System.currentTimeMillis();

		if (jobList.isEmpty())
			throw new IllegalArgumentException("监控任务Job为空");

		// 排序
		sort();

		JobFuture future = new JobFuture();
		future.setMonitorTaskId(monitorTaskId);
		future.setJobGroup(this);

		int status = 1;
		// 运行
		for (Job job : jobList) {
			long taskId = job.getMonitorTask().getTaskId();
			Timestamp currMonitorTime = job.getMonitorTask()
					.getCurrMonitorTime();
			long dataSourceId = job.getMonitorTask().getDataSourceId();
			Connection taskConn = null;
			try{
				taskConn = DbPoolManager.getConnectionForTask();
				
				MonitorTaskStatusDAO.getInstance().addMonitorTaskStatus(taskConn, taskId,
						currMonitorTime, dataSourceId);
			}finally{
				DatabaseUtil.close(taskConn);
			}

			try {
				logger.debug("监控任务{}子任务{}，运算开始", monitorTaskId, taskId);

				job.run();

				logger.debug("监控任务{}子任务{}，运算完成", monitorTaskId, taskId);
			} catch (Exception e) {
				logger.error("监控任务{}子任务{}，运算失败", new Object[]{monitorTaskId,
						taskId, e});
				status = 0;
			} finally {
				try{
					taskConn = DbPoolManager.getConnectionForTask();
					
					MonitorTaskStatusDAO.getInstance()
					.updateMonitorTaskStatusForEndRun(taskConn, taskId,
							currMonitorTime, status);
				}finally{
					DatabaseUtil.close(taskConn);
				}
			}
		}

		future.setCode(status);
		endTime = System.currentTimeMillis();
		future.setTimeConsuming((endTime - startTime));

		return future;
	}

	/**
	 * 添加子任务
	 * 
	 * @param job
	 */
	public void addJob(Job job) {
		jobList.add(job);
	}

	/**
	 * 获取监控任务Id
	 * 
	 * @return
	 */
	public String getMonitorTaskId() {
		return monitorTaskId;
	}

	/**
	 * 排序 按告警等级排序(升序),其中结单(闭环)为最后
	 */
	private void sort() {
		// STEP1:按最高告警级别自然排序
		Collections.sort(jobList);

		/**
		 * STEP2:将闭环任务放到最后
		 */
		// 2.1 先取出结单闭环任务,删除
		Job alarmProcessJob = null;
		Iterator<Job> iter = jobList.iterator();
		while (iter.hasNext()) {
			Job job = iter.next();
			if (job.getMonitorTask().getAlarmClear() == FilterBuilder.ALERM_CLEAR_FLAG) {
				alarmProcessJob = job;
				iter.remove();
				// 因为仅有一个闭环(结单)任务
				break;
			}
		}
		// 2.2 再重新添加
		if (alarmProcessJob != null) {
			jobList.add(alarmProcessJob);
		}
	}

	public List<Job> getJobList() {
		return jobList;
	}

}

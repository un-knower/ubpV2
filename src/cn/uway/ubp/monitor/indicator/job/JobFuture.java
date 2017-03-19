package cn.uway.ubp.monitor.indicator.job;

import cn.uway.ubp.monitor.task.JobGroup;

/**
 * 指标运算job线程运算结果类<br>
 * 
 * @author chenrongqiang @ 2013-6-19
 */
public class JobFuture {

	/**
	 * 执行结果码 0表示失败 1表示成功
	 */
	private int code = 0;

	/**
	 * 监控任务ID
	 */
	private String monitorTaskId;

	/**
	 * 任务组信息
	 */
	private JobGroup jobGroup;

	/**
	 * 耗时
	 */
	private long timeConsuming;

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMonitorTaskId() {
		return monitorTaskId;
	}

	public void setMonitorTaskId(String monitorTaskId) {
		this.monitorTaskId = monitorTaskId;
	}

	public long getTimeConsuming() {
		return timeConsuming;
	}

	public void setTimeConsuming(long timeConsuming) {
		this.timeConsuming = timeConsuming;
	}

	public JobGroup getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(JobGroup jobGroup) {
		this.jobGroup = jobGroup;
	}
}

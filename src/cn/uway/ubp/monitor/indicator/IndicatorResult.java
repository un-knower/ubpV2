package cn.uway.ubp.monitor.indicator;

import cn.uway.ubp.monitor.event.EventBlockData;

/**
 * IndicatorResult
 * 
 * @author chenrongqiang 2013-5-28
 */
public class IndicatorResult {

	/**
	 * 监控任务状态信息
	 */
	// protected MonitorTaskStatus monitorTaskStatus;

	/**
	 * 表达式运算状态 0失败, 1成功
	 */
	private int status;

	/**
	 * 失败原因
	 */
	private String cause;

	/**
	 * 指标表达式运算模块产生的事件
	 */
	private EventBlockData eventBlockData;

	// public MonitorTaskStatus getMonitorTaskStatus() {
	// return monitorTaskStatus;
	// }
	//
	// public void setMonitorTaskStatus(MonitorTaskStatus monitorTaskStatus) {
	// this.monitorTaskStatus = monitorTaskStatus;
	// }

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getCause() {
		return cause;
	}

	public void setCause(String cause) {
		this.cause = cause;
	}

	public EventBlockData getEventBlockData() {
		return eventBlockData;
	}

	public void setEventBlockData(EventBlockData eventBlockData) {
		this.eventBlockData = eventBlockData;
	}

}

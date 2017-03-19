package cn.uway.ubp.monitor.rule.job;

import java.util.Date;

/**
 * 规则运算模块处理结果
 * 
 * @author chenrongqiang 2013-5-28
 */
public class RuleJobFuture {

	/**
	 * 任务ID
	 */
	protected long taskId;

	/**
	 * 事件对应的数据时间
	 */
	@Deprecated
	protected Date dataTime;

	/**
	 * 开始进行规则运算的时间
	 */
	protected Date startDate;

	/**
	 * 规则计算结束的时间
	 */
	protected Date endDate;

	/**
	 * 本次规则运算产生的告警条数
	 */
	protected int alarmNum;

	/**
	 * 本次规则运算状态 0表示失败 1表示成功
	 */
	protected int status;

	/**
	 * 本次规则运算状态失败原因
	 */
	protected String cause;

	/**
	 * 告警输出成功条数
	 */
	protected int alarmExportNum;

	/**
	 * 输出失败原因
	 */
	protected String exportCause;

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public int getAlarmNum() {
		return alarmNum;
	}

	public void setAlarmNum(int alarmNum) {
		this.alarmNum = alarmNum;
	}

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

	public int getAlarmExportNum() {
		return alarmExportNum;
	}

	public void setAlarmExportNum(int alarmExportNum) {
		this.alarmExportNum = alarmExportNum;
	}

	public String getExportCause() {
		return exportCause;
	}

	public void setExportCause(String exportCause) {
		this.exportCause = exportCause;
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	@Deprecated
	public Date getDataTime() {
		return dataTime;
	}

	@Deprecated
	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}
}

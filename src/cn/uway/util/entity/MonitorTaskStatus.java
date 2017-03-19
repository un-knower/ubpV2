package cn.uway.util.entity;

import java.sql.Timestamp;

/**
 * 用于对象化数据记录：监控任务状态表
 * 
 * @author Chris @ 2014-3-31
 */
public class MonitorTaskStatus {

	private int taskId;

	private Timestamp currMonitorTime;

	private int datasourceId;

	private Timestamp expressionStartTime;

	private Timestamp expressionEndTime;

	private String expressionExperience;

	private String expressionFailCause;

	private int expressionEventNum;

	private Timestamp frequencyStartTime;

	private Timestamp frequencyEndTime;

	private int frequencyEventNum;

	private String frequencyFailCause;

	private Timestamp topnStartTime;

	private Timestamp topnEndTime;

	private int topnEventNum;

	private String topnFailCause;

	private Timestamp alarmLevelStartTime;

	private Timestamp alarmLevelEndTime;

	private int alarmLevelEventNum;

	private String alarmLevelFailCause;

	private Timestamp alarmExportStartTime;

	private Timestamp alarmExportEndTime;

	private int alarmExportNum;

	private String alarmExportFailCause;

	private Timestamp runStartTime;

	private Timestamp runEndTime;

	private boolean runStatus;

	private static final String CRLF = "\r\n";

	private static final String TAB = "\t";

	public MonitorTaskStatus(int taskId, Timestamp currMonitorTime, int datasourceId, Timestamp expressionStartTime, Timestamp expressionEndTime,
			String expressionExperience, String expressionFailCause, int expressionEventNum, Timestamp frequencyStartTime,
			Timestamp frequencyEndTime, int frequencyEventNum, String frequencyFailCause, Timestamp topnStartTime, Timestamp topnEndTime,
			int topnEventNum, String topnFailCause, Timestamp alarmLevelStartTime, Timestamp alarmLevelEndTime, int alarmLevelEventNum,
			String alarmLevelFailCause, Timestamp alarmExportStartTime, Timestamp alarmExportEndTime, int alarmExportNum,
			String alarmExportFailCause, Timestamp runStartTime, Timestamp runEndTime, int runStatus) {
		super();
		this.taskId = taskId;
		this.currMonitorTime = currMonitorTime;
		this.datasourceId = datasourceId;
		this.expressionStartTime = expressionStartTime;
		this.expressionEndTime = expressionEndTime;
		this.expressionExperience = expressionExperience;
		this.expressionFailCause = expressionFailCause;
		this.expressionEventNum = expressionEventNum;
		this.frequencyStartTime = frequencyStartTime;
		this.frequencyEndTime = frequencyEndTime;
		this.frequencyEventNum = frequencyEventNum;
		this.frequencyFailCause = frequencyFailCause;
		this.topnStartTime = topnStartTime;
		this.topnEndTime = topnEndTime;
		this.topnEventNum = topnEventNum;
		this.topnFailCause = topnFailCause;
		this.alarmLevelStartTime = alarmLevelStartTime;
		this.alarmLevelEndTime = alarmLevelEndTime;
		this.alarmLevelEventNum = alarmLevelEventNum;
		this.alarmLevelFailCause = alarmLevelFailCause;
		this.alarmExportStartTime = alarmExportStartTime;
		this.alarmExportEndTime = alarmExportEndTime;
		this.alarmExportNum = alarmExportNum;
		this.alarmExportFailCause = alarmExportFailCause;
		this.runStartTime = runStartTime;
		this.runEndTime = runEndTime;
		this.runStatus = runStatus == 1 ? true : false;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public Timestamp getCurrMonitorTime() {
		return currMonitorTime;
	}

	public void setCurrMonitorTime(Timestamp currMonitorTime) {
		this.currMonitorTime = currMonitorTime;
	}

	public int getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(int datasourceId) {
		this.datasourceId = datasourceId;
	}

	public Timestamp getExpressionStartTime() {
		return expressionStartTime;
	}

	public void setExpressionStartTime(Timestamp expressionStartTime) {
		this.expressionStartTime = expressionStartTime;
	}

	public Timestamp getExpressionEndTime() {
		return expressionEndTime;
	}

	public void setExpressionEndTime(Timestamp expressionEndTime) {
		this.expressionEndTime = expressionEndTime;
	}

	public String getExpressionExperience() {
		return expressionExperience;
	}

	public void setExpressionExperience(String expressionExperience) {
		this.expressionExperience = expressionExperience;
	}

	public String getExpressionFailCause() {
		return expressionFailCause;
	}

	public void setExpressionFailCause(String expressionFailCause) {
		this.expressionFailCause = expressionFailCause;
	}

	public int getExpressionEventNum() {
		return expressionEventNum;
	}

	public void setExpressionEventNum(int expressionEventNum) {
		this.expressionEventNum = expressionEventNum;
	}

	public Timestamp getFrequencyStartTime() {
		return frequencyStartTime;
	}

	public void setFrequencyStartTime(Timestamp frequencyStartTime) {
		this.frequencyStartTime = frequencyStartTime;
	}

	public Timestamp getFrequencyEndTime() {
		return frequencyEndTime;
	}

	public void setFrequencyEndTime(Timestamp frequencyEndTime) {
		this.frequencyEndTime = frequencyEndTime;
	}

	public int getFrequencyEventNum() {
		return frequencyEventNum;
	}

	public void setFrequencyEventNum(int frequencyEventNum) {
		this.frequencyEventNum = frequencyEventNum;
	}

	public String getFrequencyFailCause() {
		return frequencyFailCause;
	}

	public void setFrequencyFailCause(String frequencyFailCause) {
		this.frequencyFailCause = frequencyFailCause;
	}

	public Timestamp getTopnStartTime() {
		return topnStartTime;
	}

	public void setTopnStartTime(Timestamp topnStartTime) {
		this.topnStartTime = topnStartTime;
	}

	public Timestamp getTopnEndTime() {
		return topnEndTime;
	}

	public void setTopnEndTime(Timestamp topnEndTime) {
		this.topnEndTime = topnEndTime;
	}

	public int getTopnEventNum() {
		return topnEventNum;
	}

	public void setTopnEventNum(int topnEventNum) {
		this.topnEventNum = topnEventNum;
	}

	public String getTopnFailCause() {
		return topnFailCause;
	}

	public void setTopnFailCause(String topnFailCause) {
		this.topnFailCause = topnFailCause;
	}

	public Timestamp getAlarmLevelStartTime() {
		return alarmLevelStartTime;
	}

	public void setAlarmLevelStartTime(Timestamp alarmLevelStartTime) {
		this.alarmLevelStartTime = alarmLevelStartTime;
	}

	public Timestamp getAlarmLevelEndTime() {
		return alarmLevelEndTime;
	}

	public void setAlarmLevelEndTime(Timestamp alarmLevelEndTime) {
		this.alarmLevelEndTime = alarmLevelEndTime;
	}

	public int getAlarmLevelEventNum() {
		return alarmLevelEventNum;
	}

	public void setAlarmLevelEventNum(int alarmLevelEventNum) {
		this.alarmLevelEventNum = alarmLevelEventNum;
	}

	public String getAlarmLevelFailCause() {
		return alarmLevelFailCause;
	}

	public void setAlarmLevelFailCause(String alarmLevelFailCause) {
		this.alarmLevelFailCause = alarmLevelFailCause;
	}

	public Timestamp getAlarmExportStartTime() {
		return alarmExportStartTime;
	}

	public void setAlarmExportStartTime(Timestamp alarmExportStartTime) {
		this.alarmExportStartTime = alarmExportStartTime;
	}

	public Timestamp getAlarmExportEndTime() {
		return alarmExportEndTime;
	}

	public void setAlarmExportEndTime(Timestamp alarmExportEndTime) {
		this.alarmExportEndTime = alarmExportEndTime;
	}

	public int getAlarmExportNum() {
		return alarmExportNum;
	}

	public void setAlarmExportNum(int alarmExportNum) {
		this.alarmExportNum = alarmExportNum;
	}

	public String getAlarmExportFailCause() {
		return alarmExportFailCause;
	}

	public void setAlarmExportFailCause(String alarmExportFailCause) {
		this.alarmExportFailCause = alarmExportFailCause;
	}

	public Timestamp getRunStartTime() {
		return runStartTime;
	}

	public void setRunStartTime(Timestamp runStartTime) {
		this.runStartTime = runStartTime;
	}

	public Timestamp getRunEndTime() {
		return runEndTime;
	}

	public void setRunEndTime(Timestamp runEndTime) {
		this.runEndTime = runEndTime;
	}

	public boolean isRunStatus() {
		return runStatus;
	}

	public void setRunStatus(boolean runStatus) {
		this.runStatus = runStatus;
	}

	/**
	 * 返回XML节点格式信息
	 * 
	 * @return
	 */
	public String toXML() {
		StringBuilder xmlBuilder = new StringBuilder();
		xmlBuilder.append("<task>").append(CRLF);

		xmlBuilder.append(TAB).append("<task-id>").append(taskId).append("</task-id>").append(CRLF);
		xmlBuilder.append(TAB).append("<curr-monitor-time>").append(currMonitorTime).append("</curr-monitor-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<datasource-id>").append(datasourceId).append("</datasource-id>").append(CRLF);

		xmlBuilder.append(TAB).append("<expression-start-time>").append(expressionStartTime).append("</expression-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<expression-end-time>").append(expressionEndTime).append("</expression-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<expression-experience>").append("<![CDATA[").append(expressionExperience).append("]]>").append("</expression-experience>").append(CRLF);
		xmlBuilder.append(TAB).append("<expression-fail-cause>").append("<![CDATA[").append(expressionFailCause).append("]]>").append("</expression-fail-cause>").append(CRLF);
		xmlBuilder.append(TAB).append("<expression-event-num>").append(expressionEventNum).append("</expression-event-num>").append(CRLF);

		xmlBuilder.append(TAB).append("<frequency-start-time>").append(frequencyStartTime).append("</frequency-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<frequency-end-time>").append(frequencyEndTime).append("</frequency-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<frequency-event-num>").append(frequencyEventNum).append("</frequency-event-num>").append(CRLF);
		xmlBuilder.append(TAB).append("<frequency-fail-cause>").append("<![CDATA[").append(frequencyFailCause).append("]]>").append("</frequency-fail-cause>").append(CRLF);

		xmlBuilder.append(TAB).append("<topn-start-time>").append(topnStartTime).append("</topn-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<topn-end-time>").append(topnEndTime).append("</topn-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<topn-event-num>").append(topnEventNum).append("</topn-event-num>").append(CRLF);
		xmlBuilder.append(TAB).append("<topn-fail-cause>").append("<![CDATA[").append(topnFailCause).append("]]>").append("</topn-fail-cause>").append(CRLF);

		xmlBuilder.append(TAB).append("<alarm-level-start-time>").append(alarmLevelStartTime).append("</alarm-level-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-level-end-time>").append(alarmLevelEndTime).append("</alarm-level-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-level-event-num>").append(alarmLevelEventNum).append("</alarm-level-event-num>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-level-fail-cause>").append("<![CDATA[").append(alarmLevelFailCause).append("]]>").append("</alarm-level-fail-cause>").append(CRLF);

		xmlBuilder.append(TAB).append("<alarm-export-start-time>").append(alarmExportStartTime).append("</alarm-export-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-export-end-time>").append(alarmExportEndTime).append("</alarm-export-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-export-num>").append(alarmExportNum).append("</alarm-export-num>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-export-fail-cause>").append("<![CDATA[").append(alarmExportFailCause).append("]]>").append("</alarm-export-fail-cause>").append(CRLF);

		xmlBuilder.append(TAB).append("<run-start-time>").append(runStartTime).append("</run-start-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<run-end-time>").append(runEndTime).append("</run-end-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<run-status>").append(runStatus).append("</run-status>").append(CRLF);

		xmlBuilder.append("</task>").append(CRLF);

		return xmlBuilder.toString();
	}

}

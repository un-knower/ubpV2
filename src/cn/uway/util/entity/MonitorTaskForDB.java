package cn.uway.util.entity;

import java.sql.Timestamp;

/**
 * UBP监控任务定义
 * 
 * @author Chris 2013-11-4
 */
public class MonitorTaskForDB {

	/**
	 * Id(PK)
	 */
	private int taskId;

	/**
	 * 监控任务ID
	 */
	private String monitorTaskId;

	/**
	 * 任务名称
	 */
	private String taskName;

	/**
	 * 是否启用
	 */
	private int isUsed;

	/**
	 * 下次运行开始时间
	 */
	private Timestamp currRunTime;

	/**
	 * 数据结束时间
	 */
	private Timestamp endTime;

	/**
	 * 任务周期数量
	 */
	private int periodNum;

	/**
	 * 周期单位
	 */
	private String periodUnit;

	/**
	 * 调用者系统ID
	 */
	private int callerId;

	/**
	 * 节假日策略(任务规则)
	 */
	private Integer holidayPolicy;

	/**
	 * 节假日策略(数据)
	 */
	private Integer holidayStrategy;

	/**
	 * 数据过滤器定义
	 */
	private String filter;

	/**
	 * 监控任务对应的数据源信息
	 */
	private long datasourceId;

	/**
	 * 是否闭环 1:是 0:否
	 */
	private int isClear;

	/**
	 * 监控字段
	 */
	private String monitorField;

	/**
	 * 城市ID
	 */
	private int cityId;

	/**
	 * 主指标名称
	 */
	private String pimaryName;

	/**
	 * 主指标描述
	 */
	private String pimaryDescription;

	/**
	 * 电信专用
	 */
	private Integer keyIndexType;

	/**
	 * 电信专用
	 */
	private Integer tId;

	public MonitorTaskForDB(String monitorTaskId, String taskName, int isUsed, Timestamp currRunTime, Timestamp endTime, int periodNum,
			String periodUnit, int callerId, Integer holidayPolicy, Integer holidayStrategy, String filter, long datasourceId, int isClear,
			String monitorField, int cityId, String pimaryName, String pimaryDescription, Integer keyIndexType, Integer tId) {
		super();
		this.monitorTaskId = monitorTaskId;
		this.taskName = taskName;
		this.isUsed = isUsed;
		this.currRunTime = currRunTime;
		this.endTime = endTime;
		this.periodNum = periodNum;
		this.periodUnit = periodUnit;
		this.callerId = callerId;
		this.holidayPolicy = holidayPolicy;
		this.holidayStrategy = holidayStrategy;
		this.filter = filter;
		this.datasourceId = datasourceId;
		this.isClear = isClear;
		this.monitorField = monitorField;
		this.cityId = cityId;
		this.pimaryName = pimaryName;
		this.pimaryDescription = pimaryDescription;
		this.keyIndexType = keyIndexType;
		this.tId = tId;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public String getMonitorTaskId() {
		return monitorTaskId;
	}

	public void setMonitorTaskId(String monitorTaskId) {
		this.monitorTaskId = monitorTaskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public int getIsUsed() {
		return isUsed;
	}

	public void setIsUsed(int isUsed) {
		this.isUsed = isUsed;
	}

	public Timestamp getCurrRunTime() {
		return currRunTime;
	}

	public void setCurrRunTime(Timestamp currRunTime) {
		this.currRunTime = currRunTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public int getPeriodNum() {
		return periodNum;
	}

	public void setPeriodNum(int periodNum) {
		this.periodNum = periodNum;
	}

	public String getPeriodUnit() {
		return periodUnit;
	}

	public void setPeriodUnit(String periodUnit) {
		this.periodUnit = periodUnit;
	}

	public int getCallerId() {
		return callerId;
	}

	public void setCallerId(int callerId) {
		this.callerId = callerId;
	}

	public Integer getHolidayPolicy() {
		return holidayPolicy;
	}

	public void setHolidayPolicy(Integer holidayPolicy) {
		this.holidayPolicy = holidayPolicy;
	}

	public Integer getHolidayStrategy() {
		return holidayStrategy;
	}

	public void setHolidayStrategy(Integer holidayStrategy) {
		this.holidayStrategy = holidayStrategy;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public long getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(long datasourceId) {
		this.datasourceId = datasourceId;
	}

	public int getIsClear() {
		return isClear;
	}

	public void setIsClear(int isClear) {
		this.isClear = isClear;
	}

	public String getMonitorField() {
		return monitorField;
	}

	public void setMonitorField(String monitorField) {
		this.monitorField = monitorField;
	}

	public int getCityId() {
		return cityId;
	}

	public void setCityId(int cityId) {
		this.cityId = cityId;
	}

	public String getPimaryName() {
		return pimaryName;
	}

	public void setPimaryName(String pimaryName) {
		this.pimaryName = pimaryName;
	}

	public String getPimaryDescription() {
		return pimaryDescription;
	}

	public void setPimaryDescription(String pimaryDescription) {
		this.pimaryDescription = pimaryDescription;
	}

	public Integer getKeyIndexType() {
		return keyIndexType;
	}

	public void setKeyIndexType(Integer keyIndexType) {
		this.keyIndexType = keyIndexType;
	}

	public Integer gettId() {
		return tId;
	}

	public void settId(Integer tId) {
		this.tId = tId;
	}

}

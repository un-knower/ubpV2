package cn.uway.util.entity;

import cn.uway.util.enums.TimeUnit;

/**
 * 监控周期实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class MonitorPeriod {

	/**
	 * 监控周期单位
	 */
	private TimeUnit unit;

	/**
	 * 监控周期值
	 */
	private int periodNum;
	
	/**
	 * 是否是全天。0否，1是。
	 */
	private int needWhole;

	/**
	 * 指定时间
	 */
	private String assignMonitorTime;

	/**
	 * 指定时间
	 */
	private AssignTime assignTime;

	public MonitorPeriod(TimeUnit unit, int periodNum,int needWhole, String assignMonitorTime) {
		super();
		this.unit = unit;
		this.periodNum = periodNum;
		this.needWhole = needWhole;
		this.assignMonitorTime = assignMonitorTime;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public void setUnit(TimeUnit unit) {
		this.unit = unit;
	}

	public int getPeriodNum() {
		return periodNum;
	}

	public void setPeriodNum(int periodNum) {
		this.periodNum = periodNum;
	}

	public int getNeedWhole() {
		return needWhole;
	}

	public String getAssignMonitorTime() {
		return assignMonitorTime;
	}

	public void setAssignMonitorTime(String assignMonitorTime) {
		this.assignMonitorTime = assignMonitorTime;
	}

	public AssignTime getAssignTime() {
		return assignTime;
	}

	public void setAssignTime(AssignTime assignTime) {
		this.assignTime = assignTime;
	}

}

package cn.uway.util.entity;

import cn.uway.util.enums.TimeUnit;

/**
 * 分析周期实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class AnalysisPeriod {

	/**
	 * 分析周期单位
	 */
	private TimeUnit unit;

	/**
	 * 分析周期值
	 */
	private int periodNum;

	public AnalysisPeriod(TimeUnit unit, int periodNum) {
		super();
		this.unit = unit;
		this.periodNum = periodNum;
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

}

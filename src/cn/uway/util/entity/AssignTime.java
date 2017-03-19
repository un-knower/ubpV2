package cn.uway.util.entity;

import cn.uway.util.enums.TimeUnit;

/**
 * 指定时间
 * 
 * @author Chris @ 2013-11-1
 */
public class AssignTime {

	/**
	 * 指定时间的单位
	 */
	private TimeUnit unit;

	/**
	 * 指定时间
	 */
	private String value;

	public AssignTime(TimeUnit unit, String value) {
		super();
		this.unit = unit;
		this.value = value;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public void setUnit(TimeUnit unit) {
		this.unit = unit;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}

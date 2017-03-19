package cn.uway.util.entity;

import cn.uway.util.enums.TimeUnit;

/**
 * 频次规则出现次数实体类 另对应电信双频次规则
 * 
 * @author Chris @ 2013-11-1
 */
public class OccurTimes {

	/**
	 * 时间单位
	 */
	private TimeUnit unit;

	/**
	 * 出现次数
	 */
	private int value;

	/**
	 * 是否连续
	 */
	private boolean contintues;

	/**
	 * 电信双频次规则中内层频次
	 */
	private SubOccurTimes subOccurTimes;

	public OccurTimes(TimeUnit unit, int value, boolean contintues) {
		super();
		this.unit = unit;
		this.value = value;
		this.contintues = contintues;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public void setUnit(TimeUnit unit) {
		this.unit = unit;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public boolean isContintues() {
		return contintues;
	}

	public void setContintues(boolean contintues) {
		this.contintues = contintues;
	}

	public SubOccurTimes getSubOccurTimes() {
		return subOccurTimes;
	}

	public void setSubOccurTimes(SubOccurTimes subOccurTimes) {
		this.subOccurTimes = subOccurTimes;
	}

}

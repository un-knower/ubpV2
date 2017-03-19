package cn.uway.util.entity;

import java.util.List;

/**
 * 监控子任务指标描述实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class Expression {

	/**
	 * 数据范围定义
	 */
	private DataRange dataRange;

	/**
	 * 运算表达式列表
	 */
	private List<Indicator> indicatorList;

	public DataRange getDataRange() {
		return dataRange;
	}

	public void setDataRange(DataRange dataRange) {
		this.dataRange = dataRange;
	}

	public List<Indicator> getIndicatorList() {
		return indicatorList;
	}

	public void setIndicatorList(List<Indicator> indicatorList) {
		this.indicatorList = indicatorList;
	}

}

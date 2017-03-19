package cn.uway.util.entity;

import java.util.List;

/**
 * 监控子任务数据范围定义实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class DataRange {

	/**
	 * 数据范围列表
	 */
	private List<Range> rangeList;

	public List<Range> getRangeList() {
		return rangeList;
	}

	public void setRangeList(List<Range> rangeList) {
		this.rangeList = rangeList;
	}

}

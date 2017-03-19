package cn.uway.util.entity;

import java.util.List;

/**
 * 监控子任务数据范围实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class Range {

	/**
	 * 别名
	 */
	private String alias;

	/**
	 * 偏移范围列表
	 */
	private List<Offset> offsetList;

	public Range(String alias) {
		super();
		this.alias = alias;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public List<Offset> getOffsetList() {
		return offsetList;
	}

	public void setOffsetList(List<Offset> offsetList) {
		this.offsetList = offsetList;
	}

}

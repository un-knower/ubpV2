package cn.uway.util.entity;

import cn.uway.util.enums.SortWay;

/**
 * 指标排序实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class OrderIndicator {

	/**
	 * 排序方式
	 */
	private SortWay sortWay;

	/**
	 * 指标
	 */
	private String content;

	public OrderIndicator(SortWay sortWay, String content) {
		super();
		this.sortWay = sortWay;
		this.content = content;
	}

	public SortWay getSortWay() {
		return sortWay;
	}

	public void setSortWay(SortWay sortWay) {
		this.sortWay = sortWay;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}

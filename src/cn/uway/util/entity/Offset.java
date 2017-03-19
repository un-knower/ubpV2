package cn.uway.util.entity;

/**
 * 监控子任务数据偏移范围实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class Offset {

	/**
	 * 偏移起始位置
	 */
	private int from;

	/**
	 * 偏移截止位置
	 */
	private int to;

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getTo() {
		return to;
	}

	public void setTo(int to) {
		this.to = to;
	}

}

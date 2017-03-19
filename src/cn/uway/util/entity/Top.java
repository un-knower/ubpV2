package cn.uway.util.entity;

/**
 * Top实体类 TODO 目前仅支持1个指标排序，多指标排序以及多表达式排序尚未支持(-)
 * 
 * @author Chris @ 2013-11-1
 */
public class Top {

	/**
	 * TOP数量
	 */
	private int topNumber;

	/**
	 * 指标排序
	 */
	private OrderIndicator orderIndicator;

	public Top(int topNumber) {
		super();
		this.topNumber = topNumber;
	}

	public int getTopNumber() {
		return topNumber;
	}

	public void setTopNumber(int topNumber) {
		this.topNumber = topNumber;
	}

	public OrderIndicator getOrderIndicator() {
		return orderIndicator;
	}

	public void setOrderIndicator(OrderIndicator orderIndicator) {
		this.orderIndicator = orderIndicator;
	}

}

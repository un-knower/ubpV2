package cn.uway.util.entity;

/**
 * W网和G网忙时实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class BusyHourForWG {

	/**
	 * 忙时字段
	 */
	private String field;

	/**
	 * 忙时字段值
	 */
	private int value;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

}

package cn.uway.util.entity;

/**
 * 过滤器基类
 * 
 * @author Chris @ 2013-11-1
 */
public class Filterable {

	/**
	 * 过滤字段
	 */
	private String field;

	/**
	 * 过滤值
	 */
	private String value;
	
	/**
	 * 过滤器原型
	 */
	private String prototype;

	public Filterable(String field, String value) {
		super();
		this.field = field;
		this.value = value;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getPrototype() {
		return prototype;
	}

	public void setPrototype(String prototype) {
		this.prototype = prototype;
	}

}

package cn.uway.util.entity;

/**
 * 视图字段表
 * 
 * @author Chris @ 2013-11-12
 */
public class ViewField {

	/**
	 * 原始字段名称　
	 */
	private String sourceName;

	/**
	 * 转换后字段名称
	 */
	private String asName;

	/**
	 * 视图
	 */
	private String viewName;

	public ViewField(String asName, String sourceName) {
		super();
		this.asName = asName;
		this.sourceName = sourceName;
	}

	public ViewField(String sourceName, String asName, String viewName) {
		this(asName, sourceName);
		this.viewName = viewName;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getAsName() {
		return asName;
	}

	public void setAsName(String asName) {
		this.asName = asName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

}

package cn.uway.util.entity;

/**
 * 视图信息表
 * 
 * @author Chris @ 2013-11-12
 */
public class ViewInfo {

	/**
	 * 视图名　
	 */
	private String viewName;

	/**
	 * 条件
	 */
	private String viewTableCondition;

	/**
	 * 分组
	 */
	private String viewTableGroupby;

	/**
	 * 主表
	 */
	private String viewMainTalbe;

	public ViewInfo(String viewName, String viewTableCondition, String viewTableGroupby, String viewMainTalbe) {
		super();
		this.viewName = viewName;
		this.viewTableCondition = viewTableCondition;
		this.viewTableGroupby = viewTableGroupby;
		this.viewMainTalbe = viewMainTalbe;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public String getViewTableCondition() {
		return viewTableCondition;
	}

	public void setViewTableCondition(String viewTableCondition) {
		this.viewTableCondition = viewTableCondition;
	}

	public String getViewTableGroupby() {
		return viewTableGroupby;
	}

	public void setViewTableGroupby(String viewTableGroupby) {
		this.viewTableGroupby = viewTableGroupby;
	}

	public String getViewMainTalbe() {
		return viewMainTalbe;
	}

	public void setViewMainTalbe(String viewMainTalbe) {
		this.viewMainTalbe = viewMainTalbe;
	}

}

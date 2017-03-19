package cn.uway.util.entity;

/**
 * 视图表关联关系
 * 
 * @author Chris @ 2013-11-12
 */
public class ViewRelation {

	/**
	 * 表ID
	 */
	private long tableId;

	/**
	 * 关联表名称
	 */
	private String tableName;

	/**
	 * 视图
	 */
	private String viewName;

	/**
	 * 表连接
	 */
	private long connectionId;

	public ViewRelation(long tableId, String tableName, String viewName, long connectionId) {
		super();
		this.tableId = tableId;
		this.tableName = tableName;
		this.viewName = viewName;
		this.connectionId = connectionId;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public long getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(long connectionId) {
		this.connectionId = connectionId;
	}

}

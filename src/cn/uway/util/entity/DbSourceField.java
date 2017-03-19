package cn.uway.util.entity;

/**
 * 监控任务数据源字段实体类
 * 
 * @author Chris 2013-11-1
 */
public class DbSourceField {

	/**
	 * 字段是属于哪个表
	 */
	private long tableId;

	/**
	 * 字段名 对应FIELD_NAME字段
	 */
	private String name;

	/**
	 * 是否索引 对应IS_INDEX字段
	 */
	private boolean isIndex;

	/**
	 * 是否导出 对应IS_EXPORT字段
	 */
	private boolean isExport;

	/**
	 * 字段类型
	 */
	private String type;

	/**
	 * 是否监控实体 对应IS_MONITOR_ENTITY字段 20131010 留在以后优化可能使用
	 */
	// private boolean monitorEntity;

	public DbSourceField() {

	}

	public DbSourceField(long tableId, String name, int isIndex, int isExport) {
		super();
		this.tableId = tableId;
		this.name = name;
		this.isIndex = isIndex == 1 ? true : false;
		this.isExport = isExport == 1 ? true : false;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isIndex() {
		return isIndex;
	}

	public void setIndex(boolean isIndex) {
		this.isIndex = isIndex;
	}

	public boolean isExport() {
		return isExport;
	}

	public void setExport(boolean isExport) {
		this.isExport = isExport;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return this.name;
	}
}

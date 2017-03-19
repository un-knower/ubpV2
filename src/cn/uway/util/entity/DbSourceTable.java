package cn.uway.util.entity;

import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * 监控任务数据源表实体类
 * 
 * @author Chris 2013-11-1
 */
public class DbSourceTable {

	/**
	 * 表Id 对应TABLE_ID字段
	 */
	private long tableId;

	/**
	 * 表名或Sql语句别名 对应TABLE_NAME字段
	 */
	private String name;

	/**
	 * 连接Id 对应CONNECTION_ID字段
	 */
	private long connectionId;

	/**
	 * 数据源Id
	 */
	private long dataSourceId;

	/**
	 * Sql语句 对应TABLE_SQL字段
	 */
	private String sql;

	/**
	 * 添加时间
	 */
	private Timestamp addTime;

	public Timestamp getAddTime() {
		return addTime;
	}

	public void setAddTime(Timestamp addTime) {
		this.addTime = addTime;
	}

	/**
	 * 字段列表
	 */
	private List<DbSourceField> FieldList;

	public DbSourceTable() {

	}

	public DbSourceTable(long tableId, String name, long connectionId) {
		super();
		this.tableId = tableId;
		this.name = name;
		this.connectionId = connectionId;
	}

	public DbSourceTable(long tableId, String name, long connectionId,
			long dataSourceId) {
		this(tableId, name, connectionId);
		this.dataSourceId = dataSourceId;
	}

	public DbSourceTable(long tableId, String name, long connectionId,
			long dataSourceId, String sql) {
		this(tableId, name, connectionId, dataSourceId);
		this.sql = sql;
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

	public long getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(long connectionId) {
		this.connectionId = connectionId;
	}

	public long getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(long dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<DbSourceField> getFieldList() {
		return FieldList;
	}

	public void setFieldList(List<DbSourceField> fieldList) {
		this.FieldList = fieldList;
	}

	@Override
	public String toString() {
		StringBuilder table = new StringBuilder();
		if (StringUtils.isNotBlank(sql))// 支持SQL及中文字段别名
			return table.append(" (").append(sql).append(") ").append(name)
					.toString();
		else
			return table.append(name).toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DbSourceTable))
			return false;
		DbSourceTable o = (DbSourceTable) obj;
		if (o.getName().equals(this.getName()))
			return true;
		return false;
	}

	@Override
	public int hashCode() {
		return 1;
	}

}

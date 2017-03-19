package cn.uway.util.entity;

import java.util.List;

/**
 * 监控任务DB数据源实体类
 * 
 * @author Chris 2013-11-1
 */
public class DbSourceInfo {

	/**
	 * 数据源延迟加载时长 对应DATA_TIME_DELAY字段
	 */
	private int dataDelay;

	/**
	 * 数据源时间字段名称 对应TIME_FIELD字段
	 */
	private String timeFieldName;

	/**
	 * 数据源时间字段类型 对应TIME_FIELD_TYPE字段
	 */
	private String timeFieldType;

	/**
	 * 数据源时间字段所属表 对应MASTER_TABLE字段
	 */
	private String timeFieldTable;

	/**
	 * 数据源条件信息 对应CONDITION字段
	 */
	private String tableRelation;

	/**
	 * 表列表
	 */
	private List<DbSourceTable> dbSourceTableList;

	public DbSourceInfo() {

	}

	public DbSourceInfo(int dataDelay, String timeFieldName, String timeFieldType, String timeFieldTable, String tableRelation) {
		super();
		this.dataDelay = dataDelay;
		this.timeFieldName = timeFieldName;
		this.timeFieldType = timeFieldType;
		this.timeFieldTable = timeFieldTable;
		this.tableRelation = tableRelation;
	}

	public int getDataDelay() {
		return dataDelay;
	}

	public void setDataDelay(int dataDelay) {
		this.dataDelay = dataDelay;
	}

	public String getTimeFieldName() {
		return timeFieldName;
	}

	public void setTimeFieldName(String timeFieldName) {
		this.timeFieldName = timeFieldName;
	}

	public String getTimeFieldType() {
		return timeFieldType;
	}

	public void setTimeFieldType(String timeFieldType) {
		this.timeFieldType = timeFieldType;
	}

	public String getTimeFieldTable() {
		return timeFieldTable;
	}

	public void setTimeFieldTable(String timeFieldTable) {
		this.timeFieldTable = timeFieldTable;
	}

	public String getTableRelation() {
		return tableRelation;
	}

	public void setTableRelation(String tableRelation) {
		this.tableRelation = tableRelation;
	}

	public List<DbSourceTable> getDbSourceTableList() {
		return dbSourceTableList;
	}

	public void setDbSourceTableList(List<DbSourceTable> dbSourceTableList) {
		this.dbSourceTableList = dbSourceTableList;
	}

}

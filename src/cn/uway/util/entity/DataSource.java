package cn.uway.util.entity;

import java.sql.Timestamp;
import java.util.Set;

import cn.uway.util.enums.TimeUnit;

/**
 * 监控任务数据源实体类
 * 
 * @author Chris 2013-11-1
 */
public class DataSource {

	/**
	 * 数据源ID 对应datasource表ID字段
	 */
	private long id;

	/**
	 * 数据源粒度 对应datasource表GRANULARITY字段
	 */
	private TimeUnit granularity;

	/**
	 * 数据源级别 对应datasource表NE_LEVEL字段
	 */
	private String neLevel;

	/**
	 * 网络类型 对应datasource表NET_TYPE字段
	 */
	private String netType;

	/**
	 * 数据源时间
	 */
	private Timestamp dataTime;

	/**
	 * 数据源内容，SQL查询语句、表名、文件名等
	 */
	private DbSourceInfo dbSourceInfo;
	
	/**
	 * 数据源类型：datasource/file...
	 */
	private int type;
	
	/**
	 * 是否日志驱动类型
	 */
	private boolean isLogDrive;

	/**
	 * <pre>
	 * 数据源字段列表
	 * 记录数据源中所有字段,用处:
	 * 1,校验表达式或过滤器中的字段是否在列表中存在
	 * 2,添加监控任务时,入库到字段表中.之后的程序在反序列化数据时需要知道当前任务用到了哪些字段
	 * </pre>
	 */
	private Set<String> fieldSet;

	public DataSource() {

	}

	public DataSource(int id, String granularity, Timestamp dataTime, String neLevel, String netType, String tableRelation, int dataDelay,
			String timeFieldName, String timeFieldType, String timeFieldTable, int type, int isLogDrive) {
		this.id = id;
		this.granularity = TimeUnit.valueOf(granularity);
		this.dataTime = dataTime;
		this.neLevel = neLevel;
		this.netType = netType;
		
		this.dbSourceInfo = new DbSourceInfo(dataDelay, timeFieldName, timeFieldType, timeFieldTable, tableRelation);
		
		this.type = type;
		this.isLogDrive = isLogDrive == 0 ? false : true;
	}

	public long getId() {
		return id;
	}

	public void setId(long Id) {
		this.id = Id;
	}

	public TimeUnit getGranularity() {
		return granularity;
	}

	public void setGranularity(TimeUnit granularity) {
		this.granularity = granularity;
	}

	public Timestamp getDataTime() {
		return dataTime;
	}

	public void setDataTime(Timestamp dataTime) {
		this.dataTime = dataTime;
	}

	public String getNeLevel() {
		return neLevel;
	}

	public void setNeLevel(String neLevel) {
		this.neLevel = neLevel;
	}

	public String getNetType() {
		return netType;
	}

	public void setNetType(String netType) {
		this.netType = netType;
	}

	public DbSourceInfo getDbSourceInfo() {
		return dbSourceInfo;
	}

	public void setDbSourceInfo(DbSourceInfo dbSourceInfo) {
		this.dbSourceInfo = dbSourceInfo;
	}

	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public boolean isLogDrive() {
		return isLogDrive;
	}

	public void setLogDrive(boolean isLogDrive) {
		this.isLogDrive = isLogDrive;
	}

	public Set<String> getFieldSet() {
		return fieldSet;
	}

	public void setFieldSet(Set<String> fieldSet) {
		this.fieldSet = fieldSet;
	}

}

package cn.uway.util.entity;

import java.util.Date;

/**
 * 补采信息
 * 
 * @author chris @ 2014年1月14日
 */
public class RepairDataSourceInfo {

	/**
	 * 数据源ID
	 */
	private long dataSourceId;

	/**
	 * 数据时间
	 */
	private Date dataTime;

	/**
	 * 入库时间
	 */
	private Date addTime;

	/**
	 * 补采状态 0：未补采，1：已补采
	 */
	private int status;

	public RepairDataSourceInfo(long dataSourceId, Date dataTime, Date addTime) {
		super();
		this.dataSourceId = dataSourceId;
		this.dataTime = dataTime;
		this.addTime = addTime;
	}

	public long getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(long dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}

	public Date getAddTime() {
		return addTime;
	}

	public void setAddTime(Date addTime) {
		this.addTime = addTime;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

}

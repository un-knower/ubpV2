package cn.uway.ubp.monitor.data;

import cn.uway.util.entity.DataSource;

/**
 * 加载线程返回对象
 * 
 * @author chenrongqiang 2013-6-4
 */
@Deprecated
public class LoaderFuture {

	/**
	 * 数据源ID
	 */
	// protected long dataSourceId;

	/**
	 * 数据时间
	 */
	// protected Date dataTime;

	private DataSource dataSource;

	/**
	 * 数据状态 1：成功 0：失败
	 */
	private int status;

	/**
	 * 失败原因
	 */
	private String cause;

	private boolean isRepairTask;

	public boolean isRepairTask() {
		return isRepairTask;
	}

	public void setRepairTask(boolean isRepairTask) {
		this.isRepairTask = isRepairTask;
	}

	// public long getDataSourceId() {
	// return dataSourceId;
	// }
	//
	// public void setDataSourceId(long dataSourceId) {
	// this.dataSourceId = dataSourceId;
	// }
	//
	// public Date getDataTime() {
	// return dataTime;
	// }
	//
	// public void setDataTime(Date dataTime) {
	// this.dataTime = dataTime;
	// }

	public int getStatus() {
		return status;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getCause() {
		return cause;
	}

	public void setCause(String cause) {
		this.cause = cause;
	}

}

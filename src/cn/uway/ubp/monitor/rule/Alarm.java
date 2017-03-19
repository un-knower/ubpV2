package cn.uway.ubp.monitor.rule;

import java.util.List;

import cn.uway.framework.warehouse.ExportData;

/**
 * UBP 监控模块告警对象<br>
 * 
 * @author chenrongqiang @ 2013-6-22
 */
public class Alarm extends ExportData {

	/**
	 * UUID
	 */
	private static final long serialVersionUID = -8603146254543198107L;

	/**
	 * 每一条详细记录的指标值
	 */
	protected List<AlarmDetail> detailDatas;

	public List<AlarmDetail> getDetailDatas() {
		return detailDatas;
	}

	public void setDetailDatas(List<AlarmDetail> detailDatas) {
		this.detailDatas = detailDatas;
	}

}

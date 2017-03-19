package cn.uway.ubp.monitor.indicator;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.uway.ubp.monitor.data.GroupBlockData;
import cn.uway.util.entity.Indicator;

/**
 * 指标表达式运算模块参数
 * 
 * @author chenrongqiang 2013-5-29
 */
public class ExpressionParam {

	/**
	 * 表达式信息
	 */
	protected List<Indicator> indicatorList;

	/**
	 * 原始数据 key 表达式中别名alias value为一组MonitorData
	 */
	protected Map<String, GroupBlockData> rawData;

	/**
	 * 数据时间，用于生成event使用
	 */
	protected Date dataTime;

	/**
	 * 数据范围对应的数据时间
	 */
	protected Map<String, Date> dateRangeTimes;

	public List<Indicator> getIndicatorList() {
		return indicatorList;
	}

	public void setIndicatorList(List<Indicator> indicatorList) {
		this.indicatorList = indicatorList;
	}

	public Map<String, GroupBlockData> getRawData() {
		return rawData;
	}

	public void setRawData(Map<String, GroupBlockData> rawData) {
		this.rawData = rawData;
	}

	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}

	public Map<String, Date> getDateRangeTimes() {
		return dateRangeTimes;
	}

	public void setDateRangeTimes(Map<String, List<Date>> dateRangeTimes) {
		this.dateRangeTimes = new HashMap<String, Date>();
		Iterator<Entry<String, List<Date>>> iter = dateRangeTimes.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<Date>> entry = iter.next();
			if (entry != null && entry.getValue().size() > 0) {
				this.dateRangeTimes.put(entry.getKey().toLowerCase(), entry.getValue().get(0));
			}
		}
	}

	// 根据数据范围名，返回每个数据范围的开始时间
	public Date getDateByRangeName(String rangeName) {
		if (rangeName != null) {
			Date currDateRangeTime = dateRangeTimes.get(rangeName);
			if (currDateRangeTime != null)
				return currDateRangeTime;
		}

		// 默认返回计算的数据时间
		return this.dataTime;
	}

}

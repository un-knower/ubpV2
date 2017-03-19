package cn.uway.ubp.monitor.rule;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AlarmDetail {

	public static class AlarmDetailComparator implements Comparator<AlarmDetail> {

		@Override
		public int compare(AlarmDetail o1, AlarmDetail o2) {
			return o1.datetime.compareTo(o2.datetime);
		}
	}

	protected Date datetime;

	protected Map<String, Object> indicatorValues = new HashMap<String, Object>();

	public AlarmDetail() {

	}

	public Date getDatetime() {
		return datetime;
	}

	public void setDatetime(Date datetime) {
		this.datetime = datetime;
	}

	public Map<String, Object> getIndicatorValues() {
		return indicatorValues;
	}

	public void setIndicatorValues(Map<String, Object> indicatorValues) {
		this.indicatorValues = indicatorValues;
	}

}

package cn.uway.util.entity;

import java.util.List;

/**
 * 告警级别实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class AlarmLevel {

	/**
	 * 默认级别
	 */
	private short defaultLevel;

	/**
	 * 告警定义列表
	 */
	private List<Alarm> alarmList;

	public AlarmLevel(short defaultLevel) {
		super();
		this.defaultLevel = defaultLevel;
	}

	public short getDefaultLevel() {
		return defaultLevel;
	}

	public void setDefaultLevel(short defaultLevel) {
		this.defaultLevel = defaultLevel;
	}

	public List<Alarm> getAlarmList() {
		return alarmList;
	}

	public void setAlarmList(List<Alarm> alarmList) {
		this.alarmList = alarmList;
	}

	public class Alarm {

		/**
		 * 告警级别
		 */
		private short level;

		/**
		 * 满足的出现次数
		 */
		private String occurTimes;

		public Alarm(short level, String occurTimes) {
			super();
			this.level = level;
			this.occurTimes = occurTimes;
		}

		public short getLevel() {
			return level;
		}

		public void setLevel(short level) {
			this.level = level;
		}

		public String getOccurTimes() {
			return occurTimes;
		}

		public void setOccurTimes(String occurTimes) {
			this.occurTimes = occurTimes;
		}

	}

}

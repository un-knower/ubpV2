package cn.uway.util.entity;

/**
 * 监控子任务规则实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class Rule {

	/**
	 * 监控任务周期信息
	 */
	private PeriodInfo periodInfo;

	/**
	 * Top信息 可选
	 */
	private Top top;

	/**
	 * 告警级别 可选
	 */
	private AlarmLevel alarmLevel;

	public Rule() {
		super();
	}

	public Rule(PeriodInfo periodInfo, Top top, AlarmLevel alarmLevel) {
		super();
		this.periodInfo = periodInfo;
		this.top = top;
		this.alarmLevel = alarmLevel;
	}

	public PeriodInfo getPeriodInfo() {
		return periodInfo;
	}

	public void setPeriodInfo(PeriodInfo periodInfo) {
		this.periodInfo = periodInfo;
	}

	public Top getTop() {
		return top;
	}

	public void setTop(Top top) {
		this.top = top;
	}

	public AlarmLevel getAlarmLevel() {
		return alarmLevel;
	}

	public void setAlarmLevel(AlarmLevel alarmLevel) {
		this.alarmLevel = alarmLevel;
	}

}

package cn.uway.util.entity;

/**
 * 监控子任务运算表达式
 * 
 * @author Chris @ 2013-11-1
 */
public class Indicator {

	/**
	 * 告警级别
	 */
	private short alarmLevel;

	/**
	 * 运算表达式内容
	 */
	private String content;

	public short getAlarmLevel() {
		return alarmLevel;
	}

	public void setAlarmLevel(short alarmLevel) {
		this.alarmLevel = alarmLevel;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}

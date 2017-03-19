package cn.uway.ubp.monitor.rule.handler.level;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.uway.ubp.monitor.event.Event;
import cn.uway.util.entity.AlarmLevel;
import cn.uway.util.entity.AlarmLevel.Alarm;

/**
 * 告警级别处理
 * 
 * @author zqing @ 2013-8-8
 */
public class LevelRuleHandler {

	// 事件分组 key为Event中的indexValues的值 value为Event
	private Map<String, List<Event>> groupEvents;

	/**
	 * 级别定义
	 */
	private AlarmLevel alarmLevel;

	public LevelRuleHandler(AlarmLevel alarmLevel, Map<String, List<Event>> groupEvents) {
		this.alarmLevel = alarmLevel;
		this.groupEvents = groupEvents;
	}

	/**
	 * 告警级别处理
	 * 
	 * @return
	 * @throws Exception
	 */
	public Map<String, List<Event>> handle() throws Exception {
		if (alarmLevel == null)
			return groupEvents;

		short defaultAlarmLevel = alarmLevel.getDefaultLevel();

		for (Entry<String, List<Event>> eventEtry : groupEvents.entrySet()) {
			List<Event> eventList = eventEtry.getValue();
			// 一个网元对应告警事件次数
			int count = eventList.size();
			// 取配置的告警级别、表达式计算告警级两者级别高者，及取值更小的
			for (Event event : eventList) {
				// 符合配置事件次数
				boolean isHaving = false;
				for (Alarm alarm : alarmLevel.getAlarmList()) {
					// 告警级别 ，级别越高，值越小
					short alarmLevel = alarm.getLevel();
					String occurTimes = alarm.getOccurTimes();
					// 一个级别，多次数，用逗号隔开
					String[] occurTimeAry = occurTimes.split(",");
					for (String occurTime : occurTimeAry) {
						if (occurTime.equals(String.valueOf(count))) {
							// 配置的告警级别比表达式计算告警级别高
							if (event.getLevel() > alarmLevel) {
								event.setLevel(alarmLevel);
							}
							isHaving = true;
						}
					}
					// 没有找到次数告警级别，比较默认告警级别低，使用默认告警级别
					if ((!isHaving) && (event.getLevel() > defaultAlarmLevel)) {
						event.setLevel(alarmLevel);
					}
				}
			}
		}

		return groupEvents;
	}

}

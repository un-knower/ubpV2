package cn.uway.ubp.monitor.rule.handler.frequency;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cn.uway.ubp.monitor.event.Event;
import cn.uway.ubp.monitor.event.EventBlockData;
import cn.uway.ubp.monitor.rule.handler.EventProvider;
import cn.uway.util.entity.OccurTimes;

/**
 * 频次规则处理器<br>
 */
public class FrequencyRuleHandler {

	/**
	 * 告警产生的最低出现频次 默认为1
	 */
	private int occureTime = 1;

	/**
	 * 频次是否连续 默认false
	 */
	private boolean isContinue = false;

	/**
	 * 事件提供者
	 */
	private EventProvider provider;
	
	// 用于存储已经产生告警的事件
	private Map<String, List<Event>> alarmEvents;

	// 用于存储已处理的事件 index level event
	private Map<String, List<Event>> doneEvents;

	// 用于存储本次处理的事件
	private Map<String, List<Event>> currEvents;
	
	// 用于存储不满足告警条件的无效事件(用于驳回告警使用)
	private Map<String, List<Event>> invalidEvents;
	
	// 网元的回单时间
	private Map<String, Date> nesysReplayTimeMap;

	public FrequencyRuleHandler(OccurTimes occurTimes, EventProvider provider, Map<String, Date> nesysReplayTimeMap) {
		this.occureTime = occurTimes.getValue();
		this.isContinue = occurTimes.isContintues();
		this.provider = provider;
		this.nesysReplayTimeMap = nesysReplayTimeMap;
	}

	/**
	 * 频次规则处理方法<br>
	 * 迭代provider中提供的事件列表<br>
	 * 进行连续/非连续的事件出现频次处理
	 * 
	 * @return 产生告警的时间列表
	 * @throws IOException
	 *             如果读取本地事件文件失败,throws IOException
	 */
	public Map<String, List<Event>> handle() throws IOException {
		if (provider == null)
			throw new NullPointerException("事件频次处理异常.加载本地事件失败");
		int size = provider.size();
		if (occureTime > size)
			throw new IllegalArgumentException("规则配置异常，产生告警的频次定义大于实际的监控周期数量，永远无法产生告警。告警出现次数：" + occureTime + " 监控周期个数：" + size);
		EventBlockData blockData = null;
		while (provider.hasNext()) {
			blockData = provider.next();

			// 连续频次时,如果有blockData为空或者事件个数为0,需要特殊处理
			if (blockData == null || blockData.size() == 0) {
				if (isContinue && doneEvents != null) {
					// 缺少时间点的，对已经分类好的告警事件，进行规则配匹
					matchRule(doneEvents);

					// 连续性的规则，只要中间一个时间点不存在告警事件，则剩余未匹配成功的，都需要删除
					doneEvents.clear();
				}
				continue;
			}

			initialize(blockData.size());
			// 规则分类，将同一个网元的规则放在一起
			handle(blockData.getEvents());
		}

		// 最后，对已经分类好的告警记录，进行规则匹配
		matchRule(doneEvents);
		
		if (alarmEvents == null || alarmEvents.isEmpty()) {
			alarmEvents = new HashMap<String, List<Event>>();
		}
		
		// 将invalidEvents的数据，添加到alarmEvents中;
		if (invalidEvents != null && !invalidEvents.isEmpty()) {
			// this.provider.size() == 有多少个监控周期
			// this.occureTime 了== 符合条件的监控周期个数
			// rejectEventNum 不符合的监控周期个数
			int rejectEventNum = this.provider.size() - this.occureTime;
			Iterator<Entry<String, List<Event>>> iter = invalidEvents.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, List<Event>> entry = iter.next();
				String neSysid = entry.getKey();
				List<Event> eventList = entry.getValue();
				
				if (alarmEvents.containsKey(neSysid))
					continue;
				
				if (eventList.size()<1)
					continue;
				
				/*if (neSysid.equalsIgnoreCase("1100030031272200")) {
					assert(false);
				}*/
				
				// 避免因为数据缺失，和时间不够而导致驳回告警的产生
				if (eventList.size() > rejectEventNum) {
					alarmEvents.put(neSysid, eventList);
				}
			}
			
			invalidEvents.clear();
		}

		return alarmEvents;
	}

	// 从一个事件列表中，获取告警级别列表
	private List<Short> getEventsLevel(List<Event> events) {
		List<Short> levelsList = new LinkedList<Short>();
		Set<Short> levelsSet = new HashSet<Short>();
		for (Event ev : events) {
			Short level = ev.getLevel();
			if (levelsSet.contains(level))
				continue;

			levelsSet.add(level);
			levelsList.add(level);
		}

		// 对告警级别进行排序
		Collections.sort(levelsList);

		return levelsList;
	}

	// 规则匹配
	private void matchRule(Map<String, List<Event>> neSysEvents) {
		if (neSysEvents == null || neSysEvents.size() < 1)
			return;

		Iterator<Entry<String, List<Event>>> iter = neSysEvents.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, List<Event>> entry = iter.next();
			// 告警对象，网元、场景、...
			String neSysID = entry.getKey();
			// 告警事件
			List<Event> evList = entry.getValue();
			// 事件中的告警级别列表(已按升序排列)
			List<Short> levelList = getEventsLevel(evList);
			List<Event> invalidEventList = invalidEvents.get(neSysID);
			
			/*if (neSysID.equalsIgnoreCase("1100030031272200")) {
				assert(false);
			}*/
			// 对每个级别进行规则匹配(从高级别到低级别运算)
			// level值越低，告警级别越高
			for (Short level : levelList) {
				List<Event> caclEvents = new LinkedList<Event>();
				boolean bMatched = false;

				for (Event ev : evList) {
					short currLevel = ev.getLevel();
					if (currLevel == Event.INVALID_LEVEL) {
						if (level == Event.INVALID_LEVEL) {
							if (invalidEventList == null) {
								invalidEventList = new LinkedList<Event>();
								invalidEvents.put(neSysID, invalidEventList);
							}
							invalidEventList.add(ev);
						}
						continue;
					}
					// 如果当前的告警级别高于匹配的级别，则按匹配级别运算
					// 注意:level值越小，告警级别越高
					if (currLevel <= level) {
						caclEvents.add(ev);
					} else if (isContinue) {
						// 连续性的规则，只要等于或高于本次计算的事件不连续，就要去做频次匹配
						// 如果匹配不上，则就需要把本次统计的数据caclEvents清除掉，按着匹配断点后的event事件
						if (matchFrequency(neSysID, caclEvents)) {
							bMatched = true;
							break;
						} else {
							// 不连续，不能break掉，可能跳过当前时间点的也会符合规则，所以要让它接着算
							// 只要不连续，所有残存的统计数据，都要清空
							caclEvents.clear();
							continue;
						}
					}
				}

				if (!bMatched && matchFrequency(neSysID, caclEvents)) {
					bMatched = true;
				}
				
				if (bMatched) {
					invalidEvents.remove(neSysID);
					break;
				}
			}
		}
	}

	/**
	 * 获取事件列表中的告警级别 (取告警级别最低的那个)
	 * 
	 * @param evList
	 * @return
	 */
	public static short getAlarmLevel(List<Event> evList) {
		short level = Short.MIN_VALUE;
		for (Event ev : evList) {
			if (level < ev.getLevel()) {
				level = ev.getLevel();
			}
		}
		return level;
	}

	public boolean matchFrequency(String neSysID, List<Event> evList) {
		if (evList.size() >= occureTime) {
			// 如果本次告警的级别高于alarmEvents存放的级别，则替换它
			// 注意:level值越小，告警级别越高
			List<Event> alreadyAlarmEventList = alarmEvents.get(neSysID);
			if (alreadyAlarmEventList == null || getAlarmLevel(evList) < getAlarmLevel(alreadyAlarmEventList)) {
				alarmEvents.put(neSysID, evList);
			}

			return true;
		}

		return false;
	}

	/**
	 * 处理java提供的map数量大小初始值<br>
	 * double运算不精确,如果相除刚好等于0.75时，获取size会有问题,所以比例调整为0.74.在int类型最大值内都不会有问题
	 * 
	 * @param actureNum
	 * @return
	 */
	public static int size(int actureNum) {
		if (actureNum <= 12)
			return 16;
		double power = Math.log(actureNum) / Math.log(2);
		double round = Math.round(power);
		if (power < round)
			return Math.pow(2, power) / Math.pow(2, round) > 0.74 ? (int) Math.pow(2, round + 1) : (int) Math.pow(2, round);
		return Math.pow(2, power) / Math.pow(2, round + 1) > 0.74 ? (int) Math.pow(2, round + 2) : (int) Math.pow(2, round + 1);
	}

	/**
	 * 根据指定的大小初始化频次处理中的map
	 * 
	 * @param actureNum
	 */
	private void initialize(int actureNum) {
		int size = size(actureNum);

		if (alarmEvents == null)
			alarmEvents = new HashMap<>(size);
		if (currEvents == null)
			currEvents = new HashMap<>(size);
		if (doneEvents == null)
			doneEvents = new HashMap<>(size);
		if (invalidEvents == null)
			invalidEvents = new HashMap<>(size);
	}

	/**
	 * 处理一个监控周期的事件
	 * 
	 * @param events
	 *            一个监控周期的事件
	 */
	private void handle(Map<String, Event> events) {
		Iterator<String> iterator = events.keySet().iterator();
		while (iterator.hasNext()) {
			String neSysID = iterator.next();
			Event event = events.get(neSysID);
			// 回单map不为null表示该任务是闭环或者驳回
			if(null != nesysReplayTimeMap){
				// 回单时间
				Date replayDate = nesysReplayTimeMap.get(neSysID);
				// 如果回单时间为空，或者数据时间在回单时间之前，不参与运算
				if((null == replayDate)||(event.getDataTime().before(replayDate))){
					continue;
				}
			}

			List<Event> currNesysEventsList = doneEvents.remove(neSysID);
			if (currNesysEventsList == null) {
				currNesysEventsList = new LinkedList<Event>();
			}

			currNesysEventsList.add(event);
			currEvents.put(neSysID, currNesysEventsList);
		}

		// 对本次没有产生告警的网元列表doneEvents处理
		if (!isContinue) {
			// 如果不是连续条件的，则需要把doneEvents中本次没有产生告警记录的也放入到currEvents中
			currEvents.putAll(doneEvents);
		} else {
			/*
			 * 如果是连续条件的: <br/> doneEvents存放的是本次没有产生告警的网元，但存放了其它时间已经产生了告警的网元<br/>
			 * 所以此处应该将 doneEvents中本次没有产生告警的网元进行规则配置<br/>
			 */
			matchRule(doneEvents);
			// 如果doneEvents配匹后，仍有不满足规则，但本次又没产生告警的，就要将其丢弃;
			doneEvents.clear();
		}

		restoration();
	}

	/**
	 * 复位方法 将currEvents赋值给doneEvents,然后currEvents设置为Null.
	 */
	private void restoration() {
		doneEvents = null;
		doneEvents = currEvents;
		currEvents = null;
	}

}

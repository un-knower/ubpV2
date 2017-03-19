package cn.uway.ubp.monitor.rule.handler.top;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.uway.ubp.monitor.event.Event;
import cn.uway.util.entity.OrderIndicator;
import cn.uway.util.entity.Top;
import cn.uway.util.enums.SortWay;

/**
 * <pre>
 * Top规则处理
 * 这里是按指定指标排序，取前N个网元
 * 过程：
 * 1，循环所有网元、时间点，取出某指标最大值与最小值
 * 2，对步骤1的结果排序
 * 3，取TOPN个
 * @author Chris 20131012
 * </pre>
 */
public class TopRuleHandler {

	// 事件分组 key为Event中的indexValues的值 value为Event
	private Map<String, List<Event>> groupEvents = new HashMap<>();

	/**
	 * 监控规则
	 */
	private Top top;

	public TopRuleHandler(Top top, Map<String, List<Event>> groupEvents) {
		this.top = top;
		this.groupEvents = groupEvents;
	}

	/**
	 * <pre>
	 * TOPN规则处理器
	 * 目前仅实现对指标排序TOPN，以及仅支持单次排序
	 * 优先主指标，如果没有则取配置的第一个指标
	 * TODO 实现对表达式排序TOPN，以及多次排序
	 * TODO TOP规则好像有bug，待详查
	 * @return
	 * @throws IOException
	 * </pre>
	 */
	public Map<String, List<Event>> handle() throws IOException {
		class NeEve {

			String indexKey;

			Double value;
		}

		// TOPN未使用
		if (top == null)
			return groupEvents;

		// 如果频次规则运算后的监控对象个数小于TOPN的N的值，直接将频次规则运算结果输出
		if (groupEvents.size() < top.getTopNumber())
			return groupEvents;

		// TOP的处理原则 将事件进行排序 暂时不支持对结果进行avg sum等处理
		OrderIndicator orderIndicator = top.getOrderIndicator();
		String indicatorStr = orderIndicator.getContent().toLowerCase();
		final SortWay sortWay = orderIndicator.getSortWay();

		Map<String, List<Event>> groupEventTop = new HashMap<String, List<Event>>();
		ArrayList<NeEve> neEveList = new ArrayList<NeEve>();
		neEveList.ensureCapacity(groupEvents.size());

		for (Entry<String, List<Event>> entry : groupEvents.entrySet()) {
			String indexKey = entry.getKey();
			List<Event> eventList = entry.getValue();
			Double valueMax = Double.NEGATIVE_INFINITY;
			Double valueMin = Double.POSITIVE_INFINITY;
			for (Event event : eventList) {
				Map<Date, Map<String, Object>> dateEventMap = event.getIndicatorValues();
				for (Map<String, Object> eventMap : dateEventMap.values()) {
					Object val = eventMap.get(indicatorStr);
					if (!(val instanceof Number))
						continue;

					Double tempValue = ((Number) val).doubleValue();
					if (tempValue.compareTo(valueMax) > 0)
						valueMax = tempValue;
					if (tempValue.compareTo(valueMin) < 0)
						valueMin = tempValue;
				}
			}

			NeEve neEve = new NeEve();
			neEve.indexKey = indexKey;
			switch (sortWay) {
				case DESC :
					// 最大
					neEve.value = valueMax;
					break;
				default : // "asc"
					// 最小
					neEve.value = valueMin;
			}
			neEveList.add(neEve);
		}

		// 对网元的 主指标排序
		Collections.sort(neEveList, new Comparator<NeEve>() {

			public int compare(NeEve o1, NeEve o2) {
				// 如果有空值，直接返回0
				if (o1 == null || o2 == null)
					return 0;

				int result;
				switch (sortWay) {
					case DESC :
						result = (o2.value).compareTo(o1.value);
						break;
					default : // "asc"
						result = (o1.value).compareTo(o2.value);
				}

				return result;
			}
		});

		// 取出TOP N个数
		int count = 0;
		for (NeEve neEve : neEveList) {
			if (count >= top.getTopNumber())
				break;

			groupEventTop.put(neEve.indexKey, groupEvents.get(neEve.indexKey));
			count++;
		}

		return groupEventTop;
	}

}

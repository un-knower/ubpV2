package cn.uway.ubp.monitor.event;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * EventBlockData 单次任务运算产生的事件的集合
 * 
 * @author chenrongqiang 2013-5-28
 */
public class EventBlockData implements Serializable {

	/**
	 * UUID
	 */
	private static final long serialVersionUID = 3905509493345601195L;

	/**
	 * 事件对应的数据时间
	 */
	protected Date dataTime;

	/**
	 * 时间列表 key为时间的index字段组合 value为对应的事件,使用key-value的形式可以快速的查找事件
	 */
	protected Map<String, Event> events = new HashMap<String, Event>();

	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}

	/**
	 * 往EventBlockData中存入event
	 * 
	 * @param event
	 */
	public void setEvent(Event event) {
		if (event == null)
			return;
		String indexValue = event.getIndexValues();
		if (indexValue == null)
			return;
		events.put(indexValue, event);
	}

	/**
	 * 从EventBlockData查找
	 * 
	 * @param index
	 * @return
	 */
	public Event getEvent(String index) {
		return events.get(index);
	}

	/**
	 * 获取所有的event
	 * 
	 * @return
	 */
	public Map<String, Event> getEvents() {
		return events;
	}

	/**
	 * 用以判断是否包含事件
	 * 
	 * @return boolean
	 */
	public boolean isEmpty() {
		return events.size() == 0;
	}

	/**
	 * 返回EventBlockData中Event的个数
	 * 
	 * @return EventBlockData中事件的个数
	 */
	public int size() {
		return events.size();
	}
}

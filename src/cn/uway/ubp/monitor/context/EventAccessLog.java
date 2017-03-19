package cn.uway.ubp.monitor.context;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务的事件被访问的记录。<br>
 * EventAccessLog中只记录单个任务被访问的最小的数据时间
 * 
 * @author chenrongqiang @ 2013-5-27
 */
public class EventAccessLog {

	private static Map<Long, Date> EVENT_ACCESS_LOG = new HashMap<Long, Date>();

	public synchronized static void logAccess(long taskId, Date accessTime) {
		if (taskId == 0L || accessTime == null)
			return;
		Date lastLogTime = EVENT_ACCESS_LOG.get(taskId);
		// 如果访问记录不存在或者比当前时间大 则替换本次的accessTime
		if (lastLogTime == null || lastLogTime.compareTo(accessTime) > 0)
			EVENT_ACCESS_LOG.put(taskId, accessTime);
	}

	/**
	 * 查看任务访问的事件本地序列化的最小的时间
	 * 
	 * @param taskId
	 * @return
	 */
	public synchronized static Date getAccessTime(long taskId) {
		return EVENT_ACCESS_LOG.get(taskId);
	}

	/**
	 * 处理过的任务删除
	 * 
	 * @param taskId 任务编号
	 */
	public synchronized static void remove(long taskId) {
		EVENT_ACCESS_LOG.remove(taskId);
	}

	/**
	 * 返回所有数据源被访问的记录<br>
	 * 注：返回的是EVENT_ACCESS_LOG的一个克隆对象 否则可能会引起ConcurrentModifyException
	 * 
	 * @return Map<Long, Date>
	 */
	public synchronized static Map<Long, Date> getAccessLog() {
		return new HashMap<Long, Date>(EVENT_ACCESS_LOG);
	}
}

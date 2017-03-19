package cn.uway.ubp.monitor.context;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 * 单个数据源的访问记录
 * DatasourceAccessLog中只记录单个数据被访问的最小的数据时间
 * 
 * @author chenrongqiang @ 2013-5-27
 * </pre>
 */
public class DatasourceAccessLog {

	/**
	 * <pre>
	 * 数据源被访问的日期记录
	 * K:数据源ID
	 * V:数据源被访问的日期
	 * </pre>
	 */
	private static Map<Long, Date> DS_ACCESS_LOG = new HashMap<>();

	/**
	 * <pre>
	 * 在数据源被访问后记录访问事件
	 * 如果缓存中记录的访问数据时间比参数传入事件早，那么不执行更新
	 * 
	 * @param dataSourceId 需要记录访问的数据源ID
	 * @param accessTime 当前数据源访问的数据时间
	 * </pre>
	 */
	public synchronized static void logAccess(long dataSourceId, Date accessTime) {
		if (dataSourceId == 0L || accessTime == null)
			return;
		Date lastLogTime = DS_ACCESS_LOG.get(dataSourceId);
		// 如果访问记录不存在或者比当前时间小 则替换为本次的accessTime
		if (lastLogTime == null || lastLogTime.compareTo(accessTime) > 0)
			DS_ACCESS_LOG.put(dataSourceId, accessTime);
	}

	/**
	 * 处理过的数据源删除
	 * 
	 * @param dataSourceId
	 *            数据源编号
	 */
	public synchronized static void remove(long dataSourceId) {
		DS_ACCESS_LOG.remove(dataSourceId);
	}

	/**
	 * <pre>
	 * 返回所有数据源被访问的记录
	 * 注：返回的是DS_ACCESS_LOG的一个克隆对象 否则可能会引起ConcurrentModifyException
	 * 
	 * @return Map<Long, Date>
	 * </pre>
	 */
	public synchronized static Map<Long, Date> getAccessLog() {
		return new HashMap<Long, Date>(DS_ACCESS_LOG);
	}

}

package cn.uway.ubp.monitor.rule.handler;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.context.EventAccessLog;
import cn.uway.ubp.monitor.data.BlockDataProviderImpl;
import cn.uway.ubp.monitor.event.EventBlockData;
import cn.uway.ubp.monitor.event.LocalEventAccessor;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.MonitorTask;

/**
 * <pre>
 * EventProvide事件提供器
 * 1、根据事件范围从本地事件缓存中获取本地已缓存的事件
 * 2、最后输出本次从通知队列中获取的时间：即构造传入的EventBlockData
 * 
 * @author chenrongqiang 2013-5-29
 * </pre>
 */
public class EventProvider {

	/**
	 * 需要提供的事件对应的时间的迭代器
	 */
	protected Iterator<Date> dataTimeIterator;

	/**
	 * 任务对象
	 */
	protected MonitorTask monitorTask;

	/**
	 * 本次指标运算模块发送的事件信息在本地序列化文件中是不存在的
	 */
	protected EventBlockData eventBlockData;

	/**
	 * 本次指标运算模块发送的事件信息是否已经被消费
	 */
	protected boolean currEventNotUsedFlag = true;

	/**
	 * 总共有多少个EventBlockData
	 */
	protected int size = 0;

	protected LocalEventAccessor localEventAccessor = LocalEventAccessor.getInstance();

	private static final Logger logger = LoggerFactory.getLogger(EventProvider.class);

	public EventProvider() {

	}

	public EventProvider(List<Date> dataTimes, MonitorTask monitorTask, EventBlockData eventBlockData) throws Exception {
		if (dataTimes == null)
			throw new NullPointerException("加载事件事件列表为空");
		this.dataTimeIterator = dataTimes.iterator();
		size = dataTimes.size();
		this.monitorTask = monitorTask;
		try {
			localEventAccessor.lock();
			localEventAccessor.apply(getEnentPath(monitorTask.getDataSource()), monitorTask.getTaskId());
		} finally {
			localEventAccessor.unLock();
		}

		this.eventBlockData = eventBlockData;
	}

	public EventBlockData next() throws IOException {
		// if (size > 0) {
		if (dataTimeIterator.hasNext()) {
			// --size;
			Date nextEventIndexTime = dataTimeIterator.next();
			
			if(nextEventIndexTime.compareTo(eventBlockData.getDataTime())==0){
				return eventBlockData;
			}
			try {
				// LOGGER.debug("task id =={}  event datetime=={} 加载事件块文件信开始...",
				// monitorTask.getTaskId(), nextEventIndexTime);
				localEventAccessor.lock();
				localEventAccessor.apply(getEnentPath(monitorTask.getDataSource()), monitorTask.getTaskId());
				EventBlockData blockData = localEventAccessor.load(nextEventIndexTime);
				// 记录事件最后访问时间
				if (blockData != null) {
					logger.debug("task id =={}  event datetime=={} 加载事件块信息成功. 网元个数=={} ", new Object[]{monitorTask.getTaskId(), nextEventIndexTime,
							blockData.getEvents().size()});
					EventAccessLog.logAccess(monitorTask.getTaskId(), blockData.getDataTime());
				} else {
					logger.debug("task id =={}  event datetime=={} 加载事件块信息失败，事件块不存在.", monitorTask.getTaskId(), nextEventIndexTime);
				}

				return blockData;
			} finally {
				localEventAccessor.unLock();
			}
		}

		return eventBlockData;
	}

	private String getEnentPath(DataSource dataSource) {
		String eventRootFileName = BlockDataProviderImpl.getInstance().getPath(dataSource);
		return eventRootFileName;
	}

	public boolean hasNext() {
		return dataTimeIterator.hasNext();
	}

	public int size() {
		return size;
	}

}

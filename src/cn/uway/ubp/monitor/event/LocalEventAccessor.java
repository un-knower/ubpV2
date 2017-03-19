package cn.uway.ubp.monitor.event;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chenrongqiang @ 2013-6-25
 */
public class LocalEventAccessor {

	protected EventFile eventFile;

	protected EventIndexFile indexFile;

	protected ReentrantLock lock = new ReentrantLock();

	private static LocalEventAccessor localEventAccessor = new LocalEventAccessor();

	private LocalEventAccessor() {
		super();
	}

	public static LocalEventAccessor getInstance() {
		return localEventAccessor;
	}

	/**
	 * LocalEventAccessor调用锁对象
	 */
	public void lock() {
		lock.lock();
	}

	/**
	 * 调用线程显示的释放锁、将工作目录置空、关闭eventFileAccessor和indexFileAccessor<br>
	 * 如果不释放锁.其他线程将不能调用此对象
	 */
	public void unLock() {
		eventFile.close();
		indexFile.close();
		lock.unlock();
	}

	/**
	 * 设置当前操作的工作目录 并且完成初始化
	 * 
	 * @param path
	 */
	public void apply(String path, long taskId) throws IOException {
		if (path == null)
			throw new NullPointerException("[LocalEventAgent]设置工作目录失败.参数为空.");
		this.eventFile = new EventFile(path, taskId);
		this.indexFile = new EventIndexFile(path, taskId);
	}

	/**
	 * 获取最后一个event时间
	 * 
	 * @return 时间的毫秒数
	 */
	public long getLastEventTimer() {
		if (this.indexFile != null)
			return this.indexFile.getEndIndex();

		return 0;
	}

	/**
	 * 将事件序列化只本地缓存中<br>
	 * 如果文件不存在 则会创建event.bin和index.bin文件
	 * 
	 * @param blockData
	 */
	public void add(EventBlockData blockData) throws IOException {
		if (blockData == null)
			return;
		LocalEventIndex localEventIndex = eventFile.addEventBlockData(blockData);
		indexFile.addEventIndex(localEventIndex);
	}

	/**
	 * 加载指定任务单个时间点的本地事件缓存
	 * 
	 * @param taskId
	 * @param eventTime
	 * @return EventBlockData
	 * @throws IOException
	 */
	public EventBlockData load(Date dataTime) throws IOException {
		if (dataTime == null)
			return null;
		long milliseconds = dataTime.getTime();
		if (!indexFile.exists(milliseconds))
			return null;
		LocalEventIndex localEventIndex = indexFile.getEventIndex(milliseconds);
		if (localEventIndex == null)
			return null;

		return eventFile.load(localEventIndex);
	}

	/**
	 * 文件数据迁移<br>
	 * 主要提供给垃圾回收线程使用<br>
	 * 在每次迁移过程中会重新整理和排序EventFile中的信息<br>
	 * 
	 * @param dataTime
	 *            开始迁移的数据时间
	 * @throws IOException
	 */
	public void relocate(Date dataTime) throws IOException {
		long milliseconds = dataTime.getTime();
		long position = indexFile.getIndex(milliseconds);
		// 如果指定时间数据不存在 或者在起始位置 则不迁移
		if (position <= 0)
			return;
		List<LocalEventIndex> indexList = indexFile.getIndexList(position);
		eventFile.relocate(indexList);
	}

	/**
	 * 删除当前时间缓存<br>
	 * 会同时删除掉event.bin和index.bin文件
	 */
	public void delete() {
		indexFile.delete();
		eventFile.delete();
	}

	public static void main(String[] args) throws Exception {
		LocalEventAccessor agent = LocalEventAccessor.getInstance();
		agent.apply("F:/test", 1234L);
		EventBlockData data = new EventBlockData();
		long milliseconds = new Date().getTime();
		System.out.println(milliseconds);
		data.setDataTime(new Date(milliseconds));
		Event event = new Event();
		event.setIndexKey("aaaaa");
		event.setIndexValues("1434343");
		Map<Date, Map<String, Object>> indicatorValues = new HashMap<Date, Map<String, Object>>();
		// indicatorValues.put("a", 1.0);
		event.setIndicatorValues(indicatorValues);
		event.setLevel((short) 2);
		data.setEvent(event);
		agent.add(data);
		EventBlockData block = agent.load(new Date(milliseconds));
		System.out.println(block.getDataTime());
		// System.out.println(block.getEvent("1434343").getIndicatorValues().get("a"));
		/*
		 * //1370021221159 1370023153951 1370023162342 1370023169005
		 * agent.relocate(new Date(1370023162342L)); agent.unLock();
		 */
	}
}

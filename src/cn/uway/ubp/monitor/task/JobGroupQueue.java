package cn.uway.ubp.monitor.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import cn.uway.ubp.monitor.context.Configuration;

/**
 * 任务组队列<br>
 * 
 * @author Chris @ 2013-09-09
 */
public class JobGroupQueue<T> {

	/**
	 * 默认的任务组队列容量大小
	 */
	protected final int capacity;

	/**
	 * 任务组队列最大大小
	 */
	protected BlockingQueue<T> taskGroupQueue;

	public JobGroupQueue() {
		this.capacity = Configuration.getInteger(Configuration.TASK_QUEUE_CAPACITY) <= 0 ? 100 : Configuration.getInteger(Configuration.TASK_QUEUE_CAPACITY);
		taskGroupQueue = new ArrayBlockingQueue<T>(capacity);
	}

	/**
	 * 向任务组队列中放入一个任务组
	 * 
	 * @param group
	 * @throws InterruptedException
	 */
	public void put(T group) throws InterruptedException {
		if (group != null)
			taskGroupQueue.put(group);
	}

	/**
	 * 向任务组队列中放入一批任务组
	 * 
	 * @param taskGroupList
	 * @throws InterruptedException
	 */
	public void put(List<T> taskGroupList) throws InterruptedException {
		if (taskGroupList == null || taskGroupList.isEmpty())
			return;
		Iterator<T> iterator = taskGroupList.iterator();
		while (iterator.hasNext())
			put(iterator.next());
	}

	/**
	 * 从任务组队列中提取一个任务组
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public T take() throws InterruptedException {
		return taskGroupQueue.take();
	}

	/**
	 * 获取当前所有的任务组
	 * 
	 * @return 当前{@link Task}列表
	 */
	public synchronized List<T> getCurrentTaskList() {
		List<T> groups = new ArrayList<T>();
		Iterator<T> itr = taskGroupQueue.iterator();
		while (itr.hasNext()) {
			groups.add(itr.next());
		}
		return groups;
	}

	/**
	 * 清空任务组队列
	 */
	public synchronized void clear() {
		taskGroupQueue.clear();
	}

	/**
	 * 获取任务组队列的容量
	 */
	public int getCapacity() {
		return this.capacity;
	}
}

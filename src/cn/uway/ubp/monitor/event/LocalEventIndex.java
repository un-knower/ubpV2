package cn.uway.ubp.monitor.event;

/**
 * task.index.bin文件中taskId.event.bin的描述信息<br>
 * 
 * @author chenrongqiang @ 2013-6-22
 */
public class LocalEventIndex {

	/**
	 * task.index.bin中的索引标记 为时间的毫秒数
	 */
	private long index;

	/**
	 * EventBlockData在taskId.event.bin中存储的开始位置
	 */
	private long startLocation;

	/**
	 * EventBlockData在taskId.event.bin中startLocation开始存储的长度
	 */
	private long length;

	public LocalEventIndex(long index, long startLocation, long length) {
		this.index = index;
		this.startLocation = startLocation;
		this.length = length;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	public long getStartLocation() {
		return startLocation;
	}

	public void setStartLocation(long startLocation) {
		this.startLocation = startLocation;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}
}

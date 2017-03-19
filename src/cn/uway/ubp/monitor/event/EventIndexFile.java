package cn.uway.ubp.monitor.event;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;

import cn.uway.framework.util.FileUtil;
import cn.uway.framework.util.IoUtil;

/**
 * 本地事件缓存索引文件<br>
 * 1、每个时间点在index文件中占用24个字节[long+long+long]]<br>
 * 2、long存储时间的milliseconds,第二个long存储数据在event.bin中的开始位置.第三个long节存储数据的长度
 * 
 * @author chenrongqiang @ 2013-6-22
 */
public class EventIndexFile {

	/**
	 * 索引文件的RandomAccessFile访问对象
	 */
	protected RandomAccessFile indexFileAccessor;

	/**
	 * 索引文件的File对象
	 */
	protected File indexFile;

	/**
	 * 索引文件中第一个LocalEventIndex的毫秒数
	 */
	protected long startIndex = 0L;

	/**
	 * 索引文件中最后一个LocalEventIndex的毫秒数
	 */
	protected long endIndex = 0L;

	/**
	 * 索引文件的大小
	 */
	protected long length = 0L;

	/**
	 * 包含的LocalEventIndex信息的个数
	 */
	protected int size = 0;

	/**
	 * 事件和索引文件所在的目录
	 */
	protected String path;

	public EventIndexFile(String path, long taskId) throws IOException {
		if (path == null)
			throw new NullPointerException("[EventIndexFile]设置工作目录失败.参数为空.");
		this.path = path;
		initialize(taskId);
	}

	/**
	 * 检查文件目录、索引文件、事件缓存文件是否存在,如不存在则创建<br>
	 * 
	 * @param taskId
	 */
	void initialize(long taskId) throws IOException {
		if (!FileUtil.existsDirectory(path))
			FileUtil.mkdirs(path);
		// rw模式如果文件不存在 则会自动创建 所以不用判断是否已经存在文件
		this.indexFileAccessor = new RandomAccessFile(getIndexFileName(taskId), "rw");
		this.indexFile = new File(getIndexFileName(taskId));
		this.length = indexFileAccessor.length();
		this.size = (int) (this.length / 24L);
		if (length <= 0L)
			return;
		if (length % 24 != 0)
			throw new IOException("Event缓存index文件非法.");
		indexFileAccessor.seek(0L);
		this.startIndex = indexFileAccessor.readLong();
		indexFileAccessor.seek(length - 24);
		this.endIndex = indexFileAccessor.readLong();
	}

	/**
	 * 将Event索引信息添加到index文件中<br>
	 * 索引文件
	 * 
	 * @param localEventIndex
	 * @return boolean 是否添加成功
	 */
	public void addEventIndex(LocalEventIndex localEventIndex) throws IOException {
		if (localEventIndex == null)
			throw new NullPointerException("addEventIndex() Error.localEventIndex 为空");
		long index = localEventIndex.getIndex();
		// 如果原来不存在存在 则在文件末尾追加
		if (!exists(index)) {
			indexFileAccessor.seek(length);
			indexFileAccessor.writeLong(index);
			indexFileAccessor.writeLong(localEventIndex.getStartLocation());
			indexFileAccessor.writeLong(localEventIndex.getLength());
			if (this.startIndex == 0L)
				startIndex = index;
			this.endIndex = index;
			this.length += 24;
			this.size++;
			return;
		}
		// 如果以前已经存在 则更新毫秒数对应的开始位置和长度信息
		long position = getIndex(index);
		indexFileAccessor.seek(position + 8L);
		indexFileAccessor.writeLong(localEventIndex.getStartLocation());
		indexFileAccessor.writeLong(localEventIndex.getLength());
	}

	/**
	 * 删除指定的index信息<br>
	 * 只能删除第一个<br>
	 * 
	 * @param localEventIndex
	 * @throws IOException
	 */
	/*
	 * private void remove(LocalEventIndex localEventIndex) throws IOException {
	 * if (localEventIndex == null) throw new
	 * NullPointerException("remove() Error.localEventIndex 为空"); long index =
	 * localEventIndex.getIndex(); if (!exists(index)) return; if (index !=
	 * startIndex || index != endIndex) throw new
	 * UnsupportedOperationException("只能删除第一个index信息."); relocate(24); }
	 */

	/**
	 * 文件搬迁：从指定位置开始将index信息写入到一个新的文件中<br>
	 * 文件迁移需要先把原来的文件重命名,indexFileAccessor关闭<br>
	 * 
	 * @param position
	 *            文件迁移开始的位置<br>
	 * @throws IOException
	 */
	/*
	 * private boolean relocate(long position) throws IOException { //
	 * 如果position为0.不用迁移 if (position == 0L) return false; if (position % 24 !=
	 * 0) throw new IllegalArgumentException("relocate(),参数非法.position=" +
	 * position); if (position > length) throw new
	 * IllegalArgumentException("relocate(),参数错误.position=" + position +
	 * "大于文件大小" + length); long relocateLength = length - position; if
	 * (relocateLength < 24) return true; String rawIndexFileName =
	 * indexFile.getAbsolutePath() + File.separator + indexFile.getName();
	 * RandomAccessFile newIndexFileAccessor = new
	 * RandomAccessFile(rawIndexFileName + ".new", "rw"); File newIndexFile =
	 * new File(rawIndexFileName + ".new"); byte[] raw = new byte[(int)
	 * relocateLength]; // 以字节数组的方式一次读入 indexFileAccessor.seek(position);
	 * indexFileAccessor.read(raw); newIndexFileAccessor.write(raw);
	 * newIndexFileAccessor.close(); indexFileAccessor.close(); boolean
	 * indexRenameFlag = indexFile.renameTo(new File(rawIndexFileName +
	 * ".tmp")); // 如果重命名失败 整体回退 if (!indexRenameFlag) { newIndexFile.delete();
	 * return false; } indexRenameFlag = newIndexFile.renameTo(new
	 * File(rawIndexFileName)); // 新的index文件重命名失败也回滚 if (!indexRenameFlag) {
	 * newIndexFile.delete(); new File(indexFile.getName() +
	 * ".tmp").renameTo(new File(rawIndexFileName)); return false; } //
	 * 删除原来的数据文件 FileUtil.removeFile(rawIndexFileName + ".tmp"); // 重新初始化
	 * indexFileAccessor = new RandomAccessFile(rawIndexFileName, "rw");
	 * indexFile = new File(rawIndexFileName); return true; }
	 */

	/**
	 * 索引文件是否为空
	 * 
	 * @return boolean 是否为空
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * 索引文件中包含的Index个数
	 * 
	 * @return int Index信息个数
	 */
	public int size() {
		return size;
	}

	/**
	 * 判断指定毫秒数是否在index文件中存在
	 * 
	 * @param milliseconds
	 * @return
	 * @throws IOException
	 */
	public boolean exists(long milliseconds) throws IOException {
		if (milliseconds < startIndex || milliseconds > endIndex)
			return false;
		return getIndex(milliseconds) != -1L;
	}

	/**
	 * 查看指定毫秒数在index文件中的开始位置<br>
	 * 1、文件总长度、
	 * 
	 * @param milliseconds
	 * @return
	 */
	long getIndex(long milliseconds) throws IOException {
		// 文件为空
		if (length == 0L)
			return -1L;
		// 只有一条index信息
		if (startIndex == endIndex && startIndex == milliseconds)
			return 0L;
		// 只有一个元素 如果milliseconds-startIndex相等在上面逻辑范围0 此处只会返回-1
		if (size == 1)
			return -1;
		// 超出文件范围
		if (milliseconds < startIndex || milliseconds > endIndex)
			return -1L;
		// 给定时间的位置算法
		long position = getLocation(milliseconds);
		// 未找到记录
		if (position == -1)
			return -1L;
		indexFileAccessor.seek(position);
		long rawIndexValue = indexFileAccessor.readLong();
		// 如果不相等 则文件肯定不规则
		if (rawIndexValue != milliseconds)
			throw new IOException("Event缓存index文件非法.");
		return position;
	}

	/**
	 * 查找指定时间点的开始位置，如果未找到返回-1
	 * 
	 * @param milliseconds
	 * @return
	 * @throws IOException
	 */
	long getLocation(long milliseconds) throws IOException {
		long position = 0L;
		while (position <= length - 24) {
			indexFileAccessor.seek(position);
			long rawIndexValue = indexFileAccessor.readLong();
			if (rawIndexValue == milliseconds)
				return position;
			position = position + 24;
		}
		return -1L;
	}

	/**
	 * 获取指定毫秒数的索引信息<br>
	 * 注意：只适合在load的时候调用<br>
	 * 
	 * @param milliseconds
	 * @return LocalEventIndex 指定毫秒数的索引信息
	 * @throws IOException
	 */
	public LocalEventIndex getEventIndex(long milliseconds) throws IOException {
		// size肯定是24的整数倍
		if (length == 0L)
			return null;
		if (length % 24 != 0)
			throw new IOException("Event缓存index文件非法.");
		long size = length / 24;

		// 存在BUG 当数据日期被修改过，并且对应的事件文件未被删除，会出现下面异常(手工修改日期会出错，不手工修改不会出错)
		if (startIndex > milliseconds)
			throw new IllegalArgumentException("索引越界,指定时间小于本地事件存储最小时间");
		// 多于一个Index信息
		if (size > 1) {
			if (endIndex < milliseconds)
				throw new IllegalArgumentException("索引越界,指定时间大于本地事件存储最大时间");
			// long period = (endIndex - startIndex) / (size - 1);
			// 24是一个index key存放的长度
			long offset = 0;	// ((milliseconds - startIndex) / period) * 24;
			while (offset >= 0 && offset <= (length - 24)) {
				indexFileAccessor.seek(offset);
				long currMilliSecIndex = indexFileAccessor.readLong();
				if (currMilliSecIndex == milliseconds) {
					LocalEventIndex localEventIndex = new LocalEventIndex(currMilliSecIndex, indexFileAccessor.readLong(),
							indexFileAccessor.readLong());
					return localEventIndex;
				}
				offset += 24;
				// indexFileAccessor.seek(offset);
			}

			return null;
		}
		// 只有一个Index信息
		if (startIndex != milliseconds)
			return null;
		indexFileAccessor.seek(0L);
		LocalEventIndex localEventIndex = new LocalEventIndex(indexFileAccessor.readLong(), indexFileAccessor.readLong(),
				indexFileAccessor.readLong());
		return localEventIndex;
	}

	/**
	 * 输出从指定posion开始所有的LocalEventIndex信息
	 * 
	 * @param position
	 * @return List<LocalEventIndex> posion开始所有的LocalEventIndex信息.如没有则返回空的list
	 * @throws IOException
	 */
	public List<LocalEventIndex> getIndexList(long position) throws IOException {
		List<LocalEventIndex> indexList = new LinkedList<LocalEventIndex>();
		if (position >= length - 1)
			return indexList;
		indexFileAccessor.seek(position);
		while (position > length - 1) {
			LocalEventIndex LocalEventIndex = new LocalEventIndex(indexFileAccessor.readLong(), indexFileAccessor.readLong(),
					indexFileAccessor.readLong());
			indexList.add(LocalEventIndex);
			position += 24;
		}
		return indexList;
	}

	/**
	 * 根据任务ID组装索引文件名称
	 * 
	 * @param taskId
	 * @return IndexFileName
	 */
	String getIndexFileName(long taskId) {
		StringBuilder builder = new StringBuilder();
		builder.append(path).append(File.separator).append(taskId).append(".index.bin");
		return builder.toString();
	}

	/**
	 * 关闭indexFile
	 */
	public void close() {
		this.indexFile = null;
		IoUtil.close(indexFileAccessor);
	}

	public void delete() {
		IoUtil.close(indexFileAccessor);
		indexFile.delete();
	}

	public long getEndIndex() {
		return this.endIndex;
	}

}

package cn.uway.ubp.monitor.event;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;

import cn.uway.framework.util.FileUtil;
import cn.uway.framework.util.IoUtil;

/**
 * 事件缓存文件<br>
 * 以EventBlockData为粒度保存<br>
 * 
 * @author chenrongqiang @ 2013-6-22
 */
public class EventFile {

	/**
	 * 索引文件的RandomAccessFile访问对象
	 */
	protected RandomAccessFile eventFileAccessor;

	/**
	 * 索引文件的File对象
	 */
	protected File eventFile;

	/**
	 * 索引文件的大小
	 */
	protected long length = 0L;

	/**
	 * 事件文件所在的目录
	 */
	protected String path;

	public EventFile(String path, long taskId) throws IOException {
		if (path == null)
			throw new NullPointerException("[EventFile]设置工作目录失败.参数为空.");
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
		this.eventFileAccessor = new RandomAccessFile(getEventFileName(taskId), "rw");
		this.eventFile = new File(getEventFileName(taskId));
		this.length = eventFileAccessor.length();
	}

	/**
	 * 添加EventBlockData至event文件中<br>
	 * 只往尾部添加即可,不用关心顺序,垃圾回收时会自动组织文件结构<br>
	 * 
	 * @param eventBlockData
	 * @throws IOException
	 */
	public LocalEventIndex addEventBlockData(EventBlockData eventBlockData) throws IOException {
		byte[] rawEvents = getBytes(eventBlockData);
		eventFileAccessor.seek(length);
		eventFileAccessor.write(rawEvents);
		long index = eventBlockData.getDataTime().getTime();
		LocalEventIndex localEventIndex = new LocalEventIndex(index, length, rawEvents.length);
		this.length += rawEvents.length;
		return localEventIndex;
	}

	/**
	 * 文件迁移 将制定列表LocalEventIndex中所有的本地文件中的事件缓存迁移到一个新的文件中<br>
	 * 
	 * @param indexList
	 * @throws IOException
	 */
	public void relocate(List<LocalEventIndex> indexList) throws IOException {
		if (indexList == null || indexList.isEmpty())
			return;
		String rawEventFileName = eventFile.getAbsoluteFile() + File.separator + eventFile.getName();
		String newEventFileName = rawEventFileName + ".new";
		// 如果存在文件 先删除
		FileUtil.removeFile(newEventFileName);
		File newEventFile = new File(newEventFileName);
		RandomAccessFile newEventFileAccessor = new RandomAccessFile(newEventFile, "rw");
		Iterator<LocalEventIndex> iterator = indexList.iterator();
		long newLength = 0L;
		while (iterator.hasNext()) {
			LocalEventIndex localEventIndex = iterator.next();
			if (localEventIndex.getLength() == 0L)
				continue;
			eventFileAccessor.seek(localEventIndex.getStartLocation());
			byte[] rawEvent = new byte[(int) localEventIndex.getLength()];
			eventFileAccessor.read(rawEvent);
			newEventFileAccessor.seek(newLength);
			newEventFileAccessor.write(rawEvent);
			newLength += localEventIndex.getLength();
		}
		boolean renameFlag = eventFile.renameTo(new File(rawEventFileName + ".tmp"));
		newEventFileAccessor.close();
		if (!renameFlag) {
			FileUtil.removeFile(newEventFileName);
			return;
		}
		eventFileAccessor.close();
		renameFlag = newEventFile.renameTo(new File(rawEventFileName));
		if (!renameFlag) {
			new File(rawEventFileName + ".tmp").renameTo(new File(rawEventFileName));
			eventFileAccessor = new RandomAccessFile(rawEventFileName, "rw");
			return;
		}
		eventFileAccessor = new RandomAccessFile(rawEventFileName, "rw");
		eventFile = new File(rawEventFileName);
	}

	/**
	 * 加载指定任务单个时间点的本地事件缓存
	 * 
	 * @param taskId
	 * @param eventTime
	 * @return EventBlockData
	 * @throws IOException
	 */
	public EventBlockData load(LocalEventIndex localEventIndex) throws IOException {
		if (localEventIndex == null || localEventIndex.getLength() == 0L)
			return null;
		long startLocation = localEventIndex.getStartLocation();
		eventFileAccessor.seek(startLocation);
		byte[] b = new byte[(int) localEventIndex.getLength()];
		eventFileAccessor.read(b);
		Object obj = this.readObject(b);
		EventBlockData blockData = (EventBlockData) obj;
		return blockData;
	}

	/**
	 * 将存储对象转换为byte数组
	 * 
	 * @param value
	 * @return byte[]
	 */
	Object readObject(byte[] b) {
		ByteArrayInputStream byteIn = null;
		ObjectInputStream objIn = null;
		try {
			byteIn = new ByteArrayInputStream(b);
			objIn = new ObjectInputStream(byteIn);
			Object obj = objIn.readObject();
			return obj;
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		} finally {
			IoUtil.close(byteIn);
			IoUtil.close(objIn);
		}
	}

	/**
	 * 将存储对象转换为byte数组
	 * 
	 * @param value
	 * @return byte[]
	 */
	byte[] getBytes(EventBlockData blockData) {
		ByteArrayOutputStream byteOut = null;
		ObjectOutputStream objOut = null;
		try {
			byteOut = new ByteArrayOutputStream();
			objOut = new ObjectOutputStream(byteOut);
			objOut.writeObject(blockData);
			objOut.flush();
			byteOut.flush();
			return byteOut.toByteArray();
		} catch (IOException e) {
			return null;
		} finally {
			IoUtil.close(byteOut);
			IoUtil.close(objOut);
		}
	}

	public long length() {
		return length;
	}

	/**
	 * 根据任务ID组装缓存文件名称
	 * 
	 * @param taskId
	 * @return EventFileName
	 */
	String getEventFileName(long taskId) {
		StringBuilder builder = new StringBuilder();
		builder.append(path).append(File.separator).append(taskId).append(".event.bin");
		return builder.toString();
	}

	/**
	 * 关闭eventFile
	 */
	public void close() {
		this.eventFile = null;
		IoUtil.close(eventFileAccessor);
	}

	public void delete() {
		IoUtil.close(eventFileAccessor);
		eventFile.delete();
	}
}

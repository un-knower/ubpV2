package cn.uway.ubp.monitor.data.loader;

import java.sql.Timestamp;
import java.util.concurrent.Callable;

import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.ubp.monitor.data.future.LoadFuture;

/**
 * 数据源加载顶层接口
 * 
 * @author Chris 2014-3-14
 */
public interface Loader extends Callable<LoadFuture> {

	/**
	 * 加载数据并且组装成为一个BlockData
	 * 
	 * @return
	 * @throws Exception
	 */
	BlockData load() throws Exception;
	
	/**
	 * <pre>
	 * 获取可用的数据时间
	 * @param currDataTime 当前数据时间
	 * @return 返回距离当前数据时间较近或相等的可用数据时间点
	 * 	有数据源日志表
	 * 		连续数据加载时
	 * 			数据源日志表中有等于当前数据时间的记录，返回可用的数据时间为当前数据时间；没有则返回null
	 * 		离散数据加载为距离当前数据时间最近的时间；没有则返回null
	 * 	没有数据源日志表
	 * 		直接返回当前数据时间
	 * </pre>
	 */
	Timestamp getAvailableDataTime(Timestamp currDataTime) throws Exception;
	
	/**
	 * 完成数据加载的后续操作
	 */
	void finishLoad();
	
}

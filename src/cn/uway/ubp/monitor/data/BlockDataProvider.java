package cn.uway.ubp.monitor.data;

import java.util.Date;
import java.util.List;

import cn.uway.util.entity.DataSource;

/**
 * 从本地文件反序列化成数据
 * 
 * @author zhouq Date 2013-6-14
 */
public interface BlockDataProvider {

	/**
	 * 从本地文件反序列化成数据对象信息 dataTimeStr 字符串时间 为yyyyMMddHHmmss
	 * 
	 * @param dataSource
	 * @param cityID
	 * @param columns
	 * @param date
	 * @param busy
	 * @return
	 * @throws Exception
	 */
	public BlockData load(DataSource dataSource, int cityID, List<String> columns, Date date, Busy busy) throws Exception;

	/**
	 * 将BlockData序列化到文件
	 * 
	 * @param dataSource
	 * @param blockData
	 * @throws Exception
	 */
	public int save(DataSource dataSource, BlockData blockData) throws Exception;

	/**
	 * 根据数据源获取对应序列化文件目录
	 * 
	 * @param dataSource
	 * @return
	 */
	public String getPath(DataSource dataSource);
}

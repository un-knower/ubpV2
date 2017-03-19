package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Connection;

import cn.uway.ubp.monitor.data.BlockData;

/**
 * 数据过滤器<br>
 * 根據任务配置中的filter信息过滤每个数据源中的数据<br>
 * 
 * @author chenrongqiang 2013-6-14
 */
public interface IFilter {

	/**
	 * 初始化方法
	 * 
	 * @param string
	 * @throws Exception
	 */
	void init(Connection taskConn) throws Exception;

	/**
	 * 过滤方法
	 * 
	 * @param blockData
	 */
	BlockData doFilter(BlockData blockData);

	/**
	 * 销毁方法 用于过滤器中使用的资源
	 */
	void destroy();

}

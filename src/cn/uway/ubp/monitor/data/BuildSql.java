package cn.uway.ubp.monitor.data;

import java.util.List;

import cn.uway.framework.data.model.FieldInfo;
import cn.uway.util.entity.DbSourceTable;

/**
 * 根据数据信息组装查询对应连接的查询数据
 * 
 * 
 * @author zhouq Date :2013-6-13
 */
public interface BuildSql {

	/**
	 * 拼接SQL语句
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<JoinParam> buildSql() throws Exception;

	/**
	 * 获取表信息
	 * 
	 * @return
	 */
	public List<DbSourceTable> getTableInfos();

	/**
	 * 获取字段信息
	 * 
	 * @return
	 */
	public List<FieldInfo> getFieldInfos();

}

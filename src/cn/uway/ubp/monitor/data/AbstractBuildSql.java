package cn.uway.ubp.monitor.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.data.model.FieldInfo;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.ExField;

@Deprecated
public abstract class AbstractBuildSql implements BuildSql {

	/**
	 * 数据源
	 */
	protected DataSource dataSource;

	/**
	 * 字段信息中包含的索引信息
	 */
	protected List<FieldInfo> indexFieldList;

	/**
	 * 数据表信息
	 */
	protected final List<DbSourceTable> dbSourceTableList;

	/**
	 * 表对应的字段信息
	 */
	protected final List<FieldInfo> fieldList;

	/**
	 * 指定数据源以连接为键值的对应的字段信息
	 */
	protected final Map<Long, List<ExField>> fieldInfosByConnectId;

	public AbstractBuildSql(Connection taskConn, DataSource dataSource) throws SQLException {
		this.dataSource = dataSource;
		indexFieldList = new ArrayList<>();
		
		fieldList = DataSourceDAO.getInstance().getDbSourceFieldList(taskConn, dataSource.getId());
		dbSourceTableList = DataSourceDAO.getInstance().getDbSourceTableList(taskConn, dataSource.getId());
		fieldInfosByConnectId = DataSourceDAO.getInstance().getFieldListByConnection(taskConn, dataSource.getId());
		buildConditionFields();
	}

	/**
	 * <pre>
	 * 多连接组装BlockData，以数据连接为键值(支持单连接，使用单链接方法效率更快)
	 * 1.通过表和查询条件 提取组装SQL语句的元素，表字段，两张跨数据连接表的中间字段，表的查询条件信息
	 * 2.根据提取元素，组装SQL语句
	 * 3.SQL查询的先后关系。即对应的依赖顺序
	 * 4.根据SQL查询的结果组装BlockData，当为多数据连接，分数据连接组装数据
	 * 5.释放数据连接到连接池。
	 * 
	 * @return
	 * @throws Exception
	 * </pre>
	 */
	@Override
	public List<JoinParam> buildSql() throws Exception {
		if (!existIndex())
			throw new NullPointerException("索引字段至少存在一个");

		if (checkMutilConnectCodition())
			throw new NullPointerException("多连接数据条件字段不能为空！");

		preparedSql();
		creatSql();
		return joinSql();
	}

	/**
	 * Sql组装准备工作
	 * 
	 * @throws SQLException
	 */
	protected abstract void preparedSql() throws SQLException;

	/**
	 * 以连接为分组的组装
	 * 
	 * @throws Exception
	 */
	protected abstract void creatSql() throws Exception;

	/**
	 * 组装SQl语句
	 * 
	 * @return
	 */
	protected abstract List<JoinParam> joinSql();

	/**
	 * 解析 index condition 属性字段
	 */
	private void buildConditionFields() {
		for (FieldInfo field : fieldList) {
			if (field.isIndexFlag())
				indexFieldList.add(field);
		}
	}

	/**
	 * 索引字段是否存在
	 * 
	 * @return 没有为false 有为true
	 */
	private boolean existIndex() {
		return indexFieldList.size() > 0;
	}

	/**
	 * 多数据源关联条件属性为空 返回true
	 * 
	 * @return
	 */
	private boolean checkMutilConnectCodition() {
		// 这个判断是什么意思??
		if (fieldInfosByConnectId.size() <= 1)
			return false;

		return StringUtils.isBlank(dataSource.getDbSourceInfo().getTableRelation());
	}

	/**
	 * 获取表信息
	 * 
	 * @return
	 */
	public List<DbSourceTable> getTableInfos() {
		return dbSourceTableList;
	}

	/**
	 * 获取字段信息
	 * 
	 * @return
	 */
	public List<FieldInfo> getFieldInfos() {
		return fieldList;
	}

}

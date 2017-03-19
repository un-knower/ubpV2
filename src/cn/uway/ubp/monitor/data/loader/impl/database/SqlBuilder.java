package cn.uway.ubp.monitor.data.loader.impl.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.ViewInfoDAO;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.ExField;
import cn.uway.util.entity.ViewField;
import cn.uway.util.entity.ViewInfo;

/**
 * <pre>
 * SQL创建器
 * 根据监控数据源配置信息生成SQL语句
 * split方法中支持多连接语句的创建
 * @author chenrongqiang @ 2013-8-12
 * </pre>
 */
public class SqlBuilder {

	/**
	 * 数据源信息
	 */
	private DataSource dataSource;

	/**
	 * 数据源表ID和名称的对应关系 K=数据源表ID，V=数据源表名称
	 */
	private Map<Long, String> tableMapping;

	/**
	 * 已connectionID进行分组的表信息
	 */
	private Map<Long, List<DbSourceTable>> groupedTableMap;

	/**
	 * 数据源对应的字段信息
	 */
	private List<ExField> fieldInfos;

	private static final Logger logger = LoggerFactory.getLogger(SqlBuilder.class);

	public SqlBuilder(Connection conn, DataSource dataSource) throws SQLException {
		if (dataSource == null)
			throw new NullPointerException("组装SQL失败,数据源信息为空");

		this.dataSource = dataSource;
		
		initialize(conn);
	}

	/**
	 * 初始化数据源对应的表和字段信息
	 * 
	 * @throws SQLException
	 */
	private void initialize(Connection conn) throws SQLException {
		List<DbSourceTable> tableList = dataSource.getDbSourceInfo().getDbSourceTableList();
		if (tableList == null || tableList.isEmpty())
			throw new NullPointerException("组装SQL失败,数据源配置中表信息为空");
		
		fieldInfos = DataSourceDAO.getInstance().getFieldListWihView(conn, dataSource.getId());
		if (fieldInfos == null || fieldInfos.size() == 0)
			throw new NullPointerException("组装SQL失败,数据源配置中字段信息为空");
		
		// 初始化表关系（表ID和名称）映射
		tableMapping = new HashMap<>();
		for (DbSourceTable table : tableList) {
			tableMapping.put(table.getTableId(), table.getName());
		}
		
		// 将表信息以connectionId进行分组
		groupedTableMap = group(tableList);
	}

	/**
	 * <pre>
	 * 将数据源中的表信息以connectionID进行分组
	 * 
	 * @param tableInfos
	 * @return 以connectionID进行分组后的表信息
	 * </pre>
	 */
	private Map<Long, List<DbSourceTable>> group(List<DbSourceTable> tableList) {
		Map<Long, List<DbSourceTable>> groupedTableMap = new HashMap<>();

		for (DbSourceTable table : tableList) {
			List<DbSourceTable> groupTableList = groupedTableMap.get(table.getConnectionId());
			if (groupTableList == null || groupTableList.isEmpty()) {
				groupTableList = new LinkedList<>();
				groupedTableMap.put(table.getConnectionId(), groupTableList);
			}
			groupTableList.add(table);
		}

		int size = groupedTableMap.size();
		logger.debug("本次数据加载需要从{}个数据库中加载数据", size);
		if (size == 0)
			throw new NullPointerException("组装SQL失败,数据源配置中表信息为空");
		
		return groupedTableMap;
	}

	/**
	 * <pre>
	 * SQL创建器
	 * 传入指定的数据源信息
	 * create解析并以connection为分组创建sql语句
	 * 
	 * @return 以connection为分组的sql语句
	 * @throws SQLException
	 * </pre>
	 */
	public Map<Long, String> build() throws SQLException {
		int size = groupedTableMap.size();
		// 如果数据源从多个数据库中加载 则需要将sql进行拆分
		if (size > 1)
			return split();
		
		List<DbSourceTable> tableList = dataSource.getDbSourceInfo().getDbSourceTableList();
		
		Map<Long, String> sqlMap = new HashMap<>();
		// 只有一个数据库的情况 只需要将字段和条件等拼接起来
		long connectionId = tableList.get(0).getConnectionId();
		StringBuilder sql = new StringBuilder();
		sql.append("select ");
		// 拼接字段信息
		ExField exField = fieldInfos.remove(0);
		sql.append(tableMapping.get(exField.getTableId())).append(".").append(exField.getName());
		Iterator<ExField> fieldIterator = fieldInfos.iterator();
		while (fieldIterator.hasNext()) {
			exField = fieldIterator.next();
			sql.append(",").append(tableMapping.get(exField.getTableId())).append(".").append(exField.getName());
		}
		// 拼接表信息
		// TableInfo tableInfo = tableInfos.remove(0);
		// sql.append(" from ").append(tableInfo);
		sql.append(" from ");
		for (DbSourceTable table : tableList) {
			final String TIME_FLAG = "#{time}";

			String tableStr = table.toString();

			// 对于电信使用SQL数据源表的情况，支持时间过滤标签，提升查询性能
			if (StringUtils.isNotBlank(table.getSql())) {
				boolean existsFlag;

				existsFlag = tableStr.contains(TIME_FLAG);
				if (existsFlag) {
					tableStr = tableStr.replace(TIME_FLAG, "to_date('" + DateTimeUtil.formatDateTime(dataSource.getDataTime())
							+ "', 'yyyy-mm-dd hh24:mi:ss') ");
				}
			}

			sql.append(tableStr).append(",");
		}
		
		if (',' == sql.charAt(sql.length() - 1)) {
			sql.deleteCharAt(sql.length() - 1);
		}

		// 拼接条件信息
		sql.append(" where 1=1 and ");
		String tableRelation = dataSource.getDbSourceInfo().getTableRelation();
		Connection conn = null;
		try{
			conn = DbPoolManager.getConnectionForTask();
			
			if (StringUtils.isNotBlank(tableRelation)) {
				tableRelation = tableRelation.toLowerCase();
				List<ViewField> fieldNameList = DataSourceDAO.getInstance().getViewFieldByDateSourceId(conn, dataSource.getId());
				// 字段替换
				for (ViewField viewField : fieldNameList)
					tableRelation = tableRelation.replace(viewField.getAsName(), viewField.getSourceName());
				// 表名替换
				List<ViewInfo> viewInfoList = ViewInfoDAO.getInstance().getViewInfosByDataSourceId(conn, dataSource.getId());
				for (ViewInfo viewInfo : viewInfoList) {
					tableRelation = tableRelation.replace(viewInfo.getViewName(), viewInfo.getViewMainTalbe());
				}
				sql.append(tableRelation).append(" and ");
			}
			
			// 视图对应关联的实体表的关联条件
			List<DataSource> dataSourceList = DataSourceDAO.getInstance().getDataSourceWithViewInfo(conn, dataSource.getId());
			if (dataSourceList != null && !dataSourceList.isEmpty()) {
				for (DataSource dataSource : dataSourceList) {
					sql.append(dataSource.getDbSourceInfo().getTableRelation()).append(" and ");
				}
			}
		} finally {
			DatabaseUtil.close(conn);
		}

		sql.append(dataSource.getDbSourceInfo().getTimeFieldTable()).append(".").append(dataSource.getDbSourceInfo().getTimeFieldName())
				.append(" = ");

		appendTimeField(sql);
		sqlMap.put(connectionId, sql.toString());
		return sqlMap;
	}

	private void appendTimeField(StringBuilder sql) {
		String timeFieldType = dataSource.getDbSourceInfo().getTimeFieldType();
		if (!"date".equalsIgnoreCase(timeFieldType) && !"string".equalsIgnoreCase(timeFieldType))
			throw new IllegalArgumentException("组装SQL失败,无效的时间字段数据类型");

		sql.append(" ? ");
	}

	/**
	 * <pre>
	 * 拆分多数据库的复杂sql语句
	 * 拆分逻辑:
	 * 1、先替换sql中的换行符
	 * 2、处理sql中的括号优先级
	 * 3、处理and or等关联关系符:处理and时必须特殊处理between ... and ...的语法
	 * </pre>
	 */
	private Map<Long, String> split() {
		String tableRelation = dataSource.getDbSourceInfo().getTableRelation();
		if (StringUtils.isBlank(tableRelation))
			throw new IllegalArgumentException("组装SQL失败，数据源存在多个表，但是条件信息为空");
		
		// 替换条件中的换行符为空格 linux和windows中的换行符表示方式不一样
		tableRelation = tableRelation.replaceAll("\\r", " ").replaceAll("\\n", " ");
		
		// TODO 暂时不支持2,3点(-)
		throw new UnsupportedOperationException();
	}

}

package cn.uway.ubp.monitor.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.ViewFieldDAO;
import cn.uway.ubp.monitor.dao.ViewInfoDAO;
import cn.uway.ubp.monitor.dao.ViewRelationDAO;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.ExField;
import cn.uway.util.entity.ViewField;
import cn.uway.util.entity.ViewInfo;
import cn.uway.util.entity.ViewRelation;

/**
 * 
 * 多数据源查询语句
 * 
 * @author zhouq Date :2013-6-13
 */
public class MutilCreatSql extends AbstractBuildSql {

	/**
	 * 数据表信息
	 */
	private Set<DbSourceTable> newTableSet;

	/**
	 * 视图信息
	 */
	private List<ViewInfo> viewList;

	/**
	 * 表信息转换为Map方式
	 */
	private Map<String, List<DbSourceTable>> tableMap;

	/**
	 * 指定数据源以连接为键值的对应的字段信息
	 */
	private Map<Long, List<ExField>> fieldInfosWithView;

	/**
	 * 示例如下 tablea.a1=tableb.b1 and tablea.a2=tableb.b2 and tablea.a3=tablec.c1
	 * and tablea.a4=tablec.c2 and tablec.c3=tablee.e1 and tablec.c4=tablee.e2
	 * and tablee.e3 = tablef.f1 1 tableA tableB 2 tableC tableE 3 tableF
	 */

	/**
	 * tableA tableA.a5>10
	 */
	private Map<String, List<String>> oneTableCoidtionMap = new HashMap<>();

	/**
	 * tableA_talbeB tableA.a1 =tableB.b1 tableA.a2 =tableB.b2
	 * 
	 * tableA_tableC tableA.a3 =tableC.c1 tableA.a4 =tableC.c2
	 */
	// {tablee_tablef=[tablee.e3 = tablef.f1],
	// tablea_tablec=[tablea.a3=tablec.c1, tablea.a4=tablec.c2],
	// tablea_tableb=[tablea.a1=tableb.b1, tablea.a2=tableb.b2],
	// tablec_tablee=[tablec.c3=tablee.e1, tablec.c4=tablee.e2]}
	private Map<String, Set<String>> twoTableCoidtionMap = new HashMap<>();

	/**
	 * tableA tableA_tableC tableA.a3 tableA.a4 tableA_tableB tableA.a1
	 * tableA.a2
	 * 
	 */
	// {tablee={tablee_tablef=[tablee.e3 ], tablec_tablee=[tablee.e1,
	// tablee.e2]}, tableb={tablea_tableb=[tableb.b1, tableb.b2]},
	// tablec={tablea_tablec=[tablec.c1, tablec.c2], tablec_tablee=[tablec.c3,
	// tablec.c4]}, tablea={tablea_tablec=[tablea.a3, tablea.a4],
	// tablea_tableb=[tablea.a1, tablea.a2]}, tablef={tablee_tablef=[
	// tablef.f1]}}
	private Map<String, Map<String, List<String>>> extraFieldMap = new HashMap<>();

	public MutilCreatSql(Connection taskConn, DataSource dataSource) throws SQLException {
		super(taskConn, dataSource);
		
		tableMap = asTableInfo(taskConn);
		newTableSet = getNewTableInfos();
		viewList = ViewInfoDAO.getInstance().getTableInfos(taskConn);
		fieldInfosWithView = DataSourceDAO.getInstance().getFieldInfosWihView(taskConn, dataSource.getId());
	}

	/**
	 * Sql组装准备工作
	 * 
	 * @param tableInfos
	 * @param oneTableCoidtionMap
	 * @param twoTableCoidtionMap
	 * @param extraFieldMap
	 * @throws SQLException
	 */
	protected void preparedSql() throws SQLException {
		List<String> oneTablcoditoneList = null;
		// tableA.a1 =tableB.b1
		Set<String> twoTablcoditoneSet = null;
		Map<String, List<String>> extraFieldJoinTableMap = null;
		// codition tableA.a1 =tableB.b1
		for (String codition : conditionWithAnd()) {
			/*
			 * 一个等值条件 值为1 单表条件 值为2 两张表连接条件 最大为2，当为2不需要继续查找表
			 */
			int i = 0;
			String tableName = "";
			/*
			 * tableA tableB
			 */
			List<String> tableNameList = new ArrayList<String>();
			List<String> extraFieldList = null;
			/*
			 * tablea.a1=tableb.b1 {tableb=tableb.b1, tablea=tablea.a1}
			 */
			Map<String, String> tableFieldMap = new HashMap<String, String>();

			for (DbSourceTable table : newTableSet) {
				if (StringUtils.containsIgnoreCase(codition, table.getName())) {
					i++;
					// tableName = tableInfo.getTableName().toLowerCase();
					tableName = String.valueOf(table.getTableId());
					tableNameList.add(tableName);

					for (String field : codition.split("=")) {
						if (StringUtils.containsIgnoreCase(field, table.getName())) {
							tableFieldMap.put(tableName, field);
						}
					}

					if (i >= 2) {
						break;
					}
				}
			}

			// 单表 tableA.a5>10
			if (i == 1 && StringUtils.isNotBlank(tableName)) {
				oneTablcoditoneList = oneTableCoidtionMap.get(tableName);
				if (oneTablcoditoneList == null) {
					oneTablcoditoneList = new ArrayList<String>();
					oneTableCoidtionMap.put(tableName, oneTablcoditoneList);
				}
				oneTablcoditoneList.add(codition);
				// tableA.a1 =tableB.b1
			} else if (i == 2) {
				String[] tableArr = tableNameList.toArray(new String[tableNameList.size()]);
				// 排序
				Arrays.sort(tableArr);
				// tableA_tableB
				String tableJoin = StringUtils.join(tableArr, "_");
				twoTablcoditoneSet = twoTableCoidtionMap.get(tableJoin);
				if (twoTablcoditoneSet == null) {
					twoTablcoditoneSet = new HashSet<String>();
					twoTableCoidtionMap.put(tableJoin, twoTablcoditoneSet);
				}
				twoTablcoditoneSet.add(codition);

				/*
				 * 组合 extraFieldMap tableA tableA_tableC tableA.a3 tableA.a4
				 * tableA_tableB tableA.a1 tableA.a2
				 */
				for (Entry<String, String> entry : tableFieldMap.entrySet()) {
					extraFieldJoinTableMap = extraFieldMap.get(entry.getKey());
					if (extraFieldJoinTableMap == null) {
						extraFieldJoinTableMap = new HashMap<String, List<String>>();
						extraFieldMap.put(entry.getKey(), extraFieldJoinTableMap);
					}

					extraFieldList = extraFieldJoinTableMap.get(tableJoin);
					if (extraFieldList == null) {
						extraFieldList = new ArrayList<String>();
						extraFieldJoinTableMap.put(tableJoin, extraFieldList);
					}

					if (entry.getValue().indexOf("_time") > -1) {
						extraFieldList.add("to_char(" + entry.getValue() + ",'yyyy-mm-dd hh24:mi:ss')");
					} else {
						extraFieldList.add(entry.getValue());
					}
				}
			}
		}
	}

	/**
	 * 获取替换后表信息
	 * 
	 * @return
	 */
	private Set<DbSourceTable> getNewTableInfos() {
		Set<DbSourceTable> tableSet = new HashSet<DbSourceTable>();
		for (DbSourceTable table : dbSourceTableList) {
			String talbeName = table.getName().toLowerCase();
			List<DbSourceTable> talbeInfoListTemp = tableMap.get(talbeName);
			// 没有被替换表
			if (talbeInfoListTemp == null) {
				tableSet.add(table);
			} else {
				tableSet.addAll(talbeInfoListTemp);
			}
		}

		return tableSet;
	}

	/**
	 * 转化为视图信息的表结构
	 * 
	 * @return
	 * @throws SQLException
	 */
	private Map<String, List<DbSourceTable>> asTableInfo(Connection taskConn) throws SQLException {
		Map<String, List<DbSourceTable>> tableMap = new HashMap<>();
		List<ViewRelation> viewRelationList = ViewRelationDAO.getInstance().getViewRelations(taskConn);
		for (ViewRelation viewRelation : viewRelationList) {
			String viewName = viewRelation.getViewName().toLowerCase();
			List<DbSourceTable> tableInfoList = tableMap.get(viewName);
			if (tableInfoList == null) {
				tableInfoList = new ArrayList<DbSourceTable>();
				tableMap.put(viewName, tableInfoList);
			}
			DbSourceTable tableInfo = new DbSourceTable(viewRelation.getTableId(), viewRelation.getTableName(), viewRelation.getConnectionId());
			tableInfoList.add(tableInfo);
		}

		return tableMap;
	}

	/**
	 * <pre>
	 * 多连接 其中一个存在多表连接查询条件 连值条件通过 and 关键字符分割出来
	 * 
	 * @return
	 * @throws SQLException
	 * </pre>
	 */
	private List<String> conditionWithAnd() throws SQLException {
		String datasourceCondition = dataSource.getDbSourceInfo().getTableRelation().toLowerCase();
		List<String> conditionList = new ArrayList<String>();
		// 视图天剑中 视图名 ： 字段
		Map<String, List<String>> tableFieldMap = new HashMap<String, List<String>>();
		if (StringUtils.isBlank(datasourceCondition))
			return conditionList;
		// 去掉表达式中的所有换行符、tab键、回车
		String[] conditions = datasourceCondition.replaceAll("[\r\n\t]", " ").split(" and ");
		for (String condtion : conditions) {
			for (String tableField : condtion.split("=")) {
				String[] tableFiledArray = tableField.split("\\.");
				String talbe = tableFiledArray[0];
				String filed = tableFiledArray[1];
				List<String> fieldList = tableFieldMap.get(talbe);
				if (fieldList == null) {
					fieldList = new ArrayList<String>();
					tableFieldMap.put(talbe, fieldList);
				}
				fieldList.add(filed);
			}
		}

		// 视图被替换实体表条件
		Map<String, ViewInfo> viewInfoMap = asViewInfoMap();
		StringBuffer sb = new StringBuffer();
		for (Entry<String, List<String>> entry : tableFieldMap.entrySet()) {
			ViewInfo viewInfo = viewInfoMap.get(entry.getKey().toLowerCase());
			if (viewInfo != null && StringUtils.isNotBlank(viewInfo.getViewTableCondition())) {
				sb.append(viewInfo.getViewTableCondition()).append(" and ");
			}
		}

		// 视图条件被实体表替换
		List<ViewField> viewFieldList = null;
		Connection taskConn = null;
		try{
			taskConn = DbPoolManager.getConnectionForTask();
			
			viewFieldList = ViewFieldDAO.getInstance().getTableInfos(taskConn);
		}finally{
			DatabaseUtil.close(taskConn);
		}
		for (ViewField viewField : viewFieldList) {
			String asField = (viewField.getViewName() + "." + viewField.getAsName()).toLowerCase();
			if (datasourceCondition.indexOf(asField) > -1) {
				datasourceCondition = datasourceCondition.replace(asField, viewField.getSourceName());
			}
		}

		// 使用实体主表替换视图的表
		for (ViewInfo viewInfo : viewInfoMap.values()) {
			String viewName = viewInfo.getViewName().toLowerCase();
			if (datasourceCondition.indexOf(viewName) > -1) {
				datasourceCondition = datasourceCondition.replace(viewName, viewInfo.getViewMainTalbe());
			}
		}

		sb.append(datasourceCondition);
		conditions = sb.toString().replaceAll("[\r\n\t]", " ").split(" and ");
		for (String condtion : conditions) {
			conditionList.add(condtion.trim().toLowerCase());
		}

		return conditionList;
	}

	/**
	 * 转化视图为键值的视图信息
	 * 
	 * @return
	 */
	private Map<String, ViewInfo> asViewInfoMap() {
		Map<String, ViewInfo> viewInfoMap = new HashMap<String, ViewInfo>();
		for (ViewInfo viewInfo : viewList) {
			viewInfoMap.put(viewInfo.getViewName().toLowerCase(), viewInfo);
		}

		return viewInfoMap;
	}

	/**
	 * 依据准备好的查询信息组装对应连接关联的信息
	 * 
	 * @param tableInfos
	 * @param oneTableCoidtionMap
	 * @param twoTableCoidtionMap
	 * @param extraFieldMap
	 * @param extraTableByConnectMap
	 * @return
	 * @throws Exception
	 */
	protected void creatSql() throws Exception {

	}

	/**
	 * 依据连接顺序组装对应连接的SQl查询语句
	 */
	protected List<JoinParam> joinSql() {
		// 排序
		List<JoinParam> sqlList = new LinkedList<JoinParam>();
		// <conn_id, <tab_id, tab_name>>
		Map<Long, Map<Long, DbSourceTable>> tableByConnectMap = asConnectMap(newTableSet);
		// <conn_id, <tab_id, fields...>
		Map<Long, Map<Long, List<ExField>>> fieldTableMap = asFieldTable(fieldInfosWithView);
		long connectIdFirst;// 首连接Id
		String tableName = "";

		// 同一个连接中关联的表
		Set<DbSourceTable> tableNameSet = new LinkedHashSet<DbSourceTable>();
		Map<String, ViewInfo> viewInfoMap = asViewInfoMap();
		Map<String, DbSourceTable> tableMap = asTalbeMap(newTableSet);
		DbSourceInfo dbSource = dataSource.getDbSourceInfo();
		String masterTableName = dbSource.getTimeFieldTable().toLowerCase();
		ViewInfo viewInfo = viewInfoMap.get(masterTableName);
		DbSourceTable masterTableInfo = null;

		// 主表给本库连接
		if (viewInfo == null) {
			masterTableInfo = tableMap.get(masterTableName);
		} else {
			masterTableInfo = tableMap.get(viewInfo.getViewMainTalbe().toLowerCase());
		}

		if (masterTableInfo == null)
			throw new IllegalArgumentException("主表信息不存在");

		connectIdFirst = masterTableInfo.getConnectionId();
		tableName = String.valueOf(masterTableInfo.getTableId());
		tableNameSet.add(masterTableInfo);

		// 递归寻找 tableName
		findTable(connectIdFirst, tableName, tableNameSet);
		for (DbSourceTable table : tableNameSet) {
			for (Entry<Long, Map<Long, DbSourceTable>> tableEntry : tableByConnectMap.entrySet()) {
				long connectId = tableEntry.getKey();
				for (DbSourceTable _table : tableEntry.getValue().values()) {
					if (_table != table)
						continue;

					StringBuffer sbSql = new StringBuffer();
					sbSql.append("select ");
					String tableId = String.valueOf(_table.getTableId());
					Map<String, List<String>> extraMap = extraFieldMap.get(tableId);
					// 扩展字段
					for (Entry<String, List<String>> entry : extraMap.entrySet()) {
						sbSql.append(StringUtils.join(entry.getValue().iterator(), " ||'_'|| ")).append(" as joinfield_").append(entry.getKey())
								.append(",");
					}
					Map<Long, List<ExField>> connectionMap = fieldTableMap.get(connectId);
					// 该关联的连接，没有字段被使用，此连接的表可以不参与查询
					if (connectionMap == null)
						continue;

					// 指定连接的指定表的字段信息
					List<ExField> fieldInfoList = fieldTableMap.get(connectId).get(_table.getTableId());

					// 该关联的表，没有字段被使用
					if (fieldInfoList == null)
						continue;

					// 表字段
					sbSql.append(StringUtils.join(fieldTableMap.get(connectId).get(_table.getTableId()).iterator(), " , "));
					sbSql.append(" from ");

					// 表名
					sbSql.append(_table.getName());
					sbSql.append(" where 1=1");

					if (StringUtils.equalsIgnoreCase(_table.getName().toLowerCase(), masterTableInfo.getName())) {
						// 查找主表
						sbSql.append(" and ");
						String dateStr = dbSource.getTimeFieldName().toLowerCase();
						// 查找字段包含AS 时间查询字段替换
						for (ExField field : fieldInfoList) {
							String fieldName = field.getName().toLowerCase();
							if ((fieldName.indexOf(dateStr) > -1) && (fieldName.indexOf("as") > -1)) {
								dateStr = fieldName.substring(0, fieldName.indexOf("as"));
							}
						}

						switch (dbSource.getTimeFieldType()) {
							case FieldType.STRING :
							case FieldType.DATE :
								sbSql.append(dateStr).append("=").append("?").toString();
								break;
							default :
								throw new IllegalArgumentException("数据时间字段类型只能为String或Date类型");
						}
					} else {
						// 查找子表的时间字段
						String timeField = null;
						String masterTimeField = masterTableName + "." + dbSource.getTimeFieldName().toLowerCase();
						String joinKey = masterTableInfo.getTableId() + "_" + _table.getTableId();
						Set<String> joinCondition = twoTableCoidtionMap.get(joinKey);
						if (joinCondition != null) {
							for (String condition : joinCondition) {
								String[] compareKeys = condition.split("=");
								int i = 0;
								for (String key : compareKeys) {
									key = key.trim();
									// 找到和主表时间join的字段名，然后再加上时间限制，不然表在join的时候会因为数据量太大而很慢
									if (key.compareToIgnoreCase(masterTimeField) == 0) {
										if (i == 0)
											timeField = compareKeys[1];
										else
											timeField = compareKeys[0];

										// TODO:主从表的字段属性(类型)不一样，那么还有BUG.(-)
										sbSql.append(" and ");
										sbSql.append(timeField).append("=").append("?").toString();

										break;
									}

									++i;
								}

								if (timeField != null)
									break;
							}
						}
					}

					List<String> oneCondtionList = oneTableCoidtionMap.get(String.valueOf(_table.getTableId()));
					if (oneCondtionList != null && oneCondtionList.size() > 0) {
						for (String oneCondtion : oneCondtionList) {
							sbSql.append(" and ").append(oneCondtion);
						}
					}
					JoinParam joinParam = new JoinParam();
					joinParam.setConnectId(connectId);
					joinParam.setSql(sbSql.toString());
					sqlList.add(joinParam);
					break;
				}
			}
		}

		return sqlList;
	}

	/**
	 * 查找所有表记录
	 * 
	 * @param connectId
	 * @param tableName
	 * @param tableSet
	 */
	private void findTable(long connectId, String tableName, Set<DbSourceTable> tableSet) {
		for (String talbeCombination : twoTableCoidtionMap.keySet()) {
			List<String> talbeCombinationList = Arrays.asList(talbeCombination.split("_"));
			if (talbeCombinationList.contains(tableName)) {
				for (DbSourceTable table : newTableSet) {
					String tableNameTemp = String.valueOf(table.getTableId());
					if (talbeCombinationList.contains(tableNameTemp) && (!tableName.equalsIgnoreCase(tableNameTemp)) && !tableSet.contains(table)) {
						tableSet.add(table);
						findTable(connectId, tableNameTemp, tableSet);
					}
				}
			}
		}
	}

	/**
	 * <pre>
	 * List To MAp
	 * 
	 * @param tableSet
	 * @return
	 * </pre>
	 */
	private Map<String, DbSourceTable> asTalbeMap(Set<DbSourceTable> tableSet) {
		Map<String, DbSourceTable> tableMap = new HashMap<>();
		for (DbSourceTable table : tableSet) {
			tableMap.put(table.getName().toLowerCase(), table);
		}

		return tableMap;
	}

	/**
	 * <pre>
	 * 以连接为键值的数据表结构信息 connectId tableId TableInfo
	 * 
	 * @param tableSet
	 * @return
	 * </pre>
	 */
	Map<Long, Map<Long, DbSourceTable>> asConnectMap(Set<DbSourceTable> tableSet) {
		Map<Long, Map<Long, DbSourceTable>> connetMap = new HashMap<>();
		Map<Long, DbSourceTable> tableMap = null;

		for (DbSourceTable table : tableSet) {
			long connectId = table.getConnectionId();
			tableMap = connetMap.get(connectId);
			if (tableMap == null) {
				tableMap = new HashMap<Long, DbSourceTable>();
				connetMap.put(connectId, tableMap);
			}
			tableMap.put(table.getTableId(), table);
		}

		return connetMap;
	}

	/**
	 * <pre>
	 * 转换为含有表结构 connectId tableId fieldInfo
	 * 
	 * @param fieldMap
	 * @return
	 * </pre>
	 */
	Map<Long, Map<Long, List<ExField>>> asFieldTable(Map<Long, List<ExField>> fieldMap) {
		Map<Long, Map<Long, List<ExField>>> fieldTalbeMap = new HashMap<>();

		for (Entry<Long, List<ExField>> entry : fieldMap.entrySet()) {
			Map<Long, List<ExField>> tableMap = fieldTalbeMap.get(entry.getKey());
			if (tableMap == null) {
				tableMap = new HashMap<Long, List<ExField>>();
				fieldTalbeMap.put(entry.getKey(), tableMap);
			}

			List<ExField> fieldList = entry.getValue();
			for (ExField field : fieldList) {
				List<ExField> exFieldList = tableMap.get(field.getTableId());
				if (exFieldList == null) {
					exFieldList = new ArrayList<ExField>();
					tableMap.put(field.getTableId(), exFieldList);
				}

				exFieldList.add(field);
			}
		}

		return fieldTalbeMap;
	}

}

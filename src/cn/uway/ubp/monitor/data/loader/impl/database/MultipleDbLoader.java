package cn.uway.ubp.monitor.data.loader.impl.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.connection.pool.db.DBPool;
import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldInfo;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.framework.util.database.ResultSetHelper;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.ConnectInfoDAO;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.ubp.monitor.data.JoinParam;
import cn.uway.ubp.monitor.data.MutilCreatSql;
import cn.uway.util.entity.DbSourceInfo;

/**
 * 多库方式的数据加载器
 * TODO 多库方式数据源加载需要完善
 * @author Chris 2014-3-11
 */
public class MultipleDbLoader extends DbLoader {

	private static final String JoinKeyPrefixName = "joinfield_";

	private static final Logger logger = LoggerFactory.getLogger(MultipleDbLoader.class);

	public MultipleDbLoader(cn.uway.util.entity.DataSource dataSource) {
		super(dataSource);
	}

	public BlockData load() throws Exception {
		long dataSourceId = dataSource.getId();
		logger.debug("数据源{}，开始加载数据，数据时间{}，主表{}", new Object[]{dataSourceId, dataSource.getDataTime(),
				dataSource.getDbSourceInfo().getTimeFieldTable()});

		MutilCreatSql buildSql = null;
		Connection taskConn = null;
		List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();
		try{
			taskConn = DbPoolManager.getConnectionForTask();
			
			buildSql = new MutilCreatSql(taskConn, dataSource);
			fieldInfoList = DataSourceDAO.getInstance().getDbSourceFieldList(taskConn, dataSourceId);
		}finally{
			DatabaseUtil.close(taskConn);
		}
		List<JoinParam> joinParams = buildSql.buildSql();
		// 获取当前数据源的所有字段信息

		BlockData blockData = new BlockData();
		List<GroupingArrayData> groupingArrayDataList = new LinkedList<GroupingArrayData>();
		GroupingArrayDataDescriptor metaInfo = new GroupingArrayDataDescriptor();
		blockData.setMetaInfo(metaInfo);

		// 索引字段列表
		List<FieldInfo> fieldList = new ArrayList<FieldInfo>();
		// 索引字段在GroupingArrayData中的存储信息列表
		List<FieldIndexInfo> fieldIndexList = new ArrayList<FieldIndexInfo>();
		// 导出字段列表
		List<FieldInfo> exportFieldList = new ArrayList<FieldInfo>();

		// 字段在GroupingArrayData中的存储信息列表
		FieldIndexInfo[][] fieldIndexInfos = new FieldIndexInfo[joinParams.size()][];

		// 表与表之间，连接关系map
		// key:连接表名(joinfield_表1的ID_表2的ID);
		// value: 连接索引
		// key: 连接外键的值，如NE_CELL_ID(20123522512322)
		// value: GroupingArrayData的NE_CELL_ID == 20123522512322数据集合
		Map<String, Map<String, List<GroupingArrayData>>> mapKeys = new HashMap<String, Map<String, List<GroupingArrayData>>>();

		try {
			// 创建数据库连接，根据所有查询字段的属性，建立好GroupingArrayDataDescriptor对象及列存储属性
			int tabIndex = -1;
			Iterator<JoinParam> joinIter = joinParams.iterator();
			while (joinIter.hasNext()) {
				JoinParam joinParma = joinIter.next();
				++tabIndex;

				// 创建数据链接
				Connection conn = DBPool.getConnection(joinParma.getConnectId(), false);
				PreparedStatement statement = null;
				ResultSet rs = null;

				if (conn == null) {
					// 如果连接池没有，则创建新连接
					try{
						taskConn = DbPoolManager.getConnectionForTask();
						
						DatabaseConnectionInfo databaseConnectionInfo = ConnectInfoDAO.getInstance().getDatabaseConnectionInfo(taskConn, joinParma.getConnectId());
						// 如果连接创建成功，createDataSource会自动放进连接池内
						DataSource dataSource = DBPool.createDataSource(databaseConnectionInfo);
						if (dataSource != null)
							conn = dataSource.getConnection();
					}finally{
						DatabaseUtil.close(taskConn);
					}
				}

				if (conn == null)
					throw new Exception("创建数据库连接失败. connID=" + joinParma.getConnectId());

				// 设置参数
				String sql = joinParma.getSql();
				if (sql.indexOf('?') >= 0) {
					// TODO:主从表的字段属性(类型)不一样，那么还有BUG.(-)
					DbSourceInfo dbSource = dataSource.getDbSourceInfo();
					switch (dbSource.getTimeFieldType().toLowerCase()) {
						case FieldType.STRING :
							sql = sql.replace("?", "'" + DateTimeUtil.formatDateTime(dataSource.getDataTime()) + "' ");
							break;
						case FieldType.DATE :
							sql = sql.replace("?", "to_date('" + DateTimeUtil.formatDateTime(dataSource.getDataTime())
									+ "', 'yyyy-mm-dd hh24:mi:ss') ");
							break;
						default :
							throw new IllegalArgumentException("数据时间字段类型只能为String或Date类型");
					}
				}

				// 查询SQL
				statement = conn.prepareStatement(sql);

				logger.debug("datasourceId={}开始加载dataTime={}的执行SQL语句={}",
						new Object[]{dataSource.getId(), dataSource.getDataTime(), joinParma.getSql()});
				rs = statement.executeQuery();
				joinParma.setRs(rs);

				// 从MetaData对象中，创建GroupingArrayDataDescriptor对象。
				ResultSetMetaData metaData = rs.getMetaData();
				int fieldCount = metaData.getColumnCount();

				// 根据数据库的查询结果，填充表头类型等相关描述信息;
				fieldIndexInfos[tabIndex] = new FieldIndexInfo[fieldCount];
				for (int i = 1; i <= fieldCount; ++i) {
					String fieldName = metaData.getColumnName(i).toLowerCase();
					if (fieldName.indexOf(JoinKeyPrefixName) == 0)
						continue;

					FieldInfo currFieldInfo = null;
					// 找出对应的字段信息描述
					for (FieldInfo field : fieldInfoList) {
						if (field.getName().equalsIgnoreCase(fieldName)) {
							currFieldInfo = field;
							break;
						}
					}

					if (currFieldInfo == null)
						continue;

					// 根据数据库sql字段查询属性设置每个字段类型
					currFieldInfo.setType(ResultSetHelper.getFieldType(rs, currFieldInfo.getName()));
				}
			}

			// 将所有的字段 ，按sql查询出来的order顺序 注册到metaInfo中，并找出indexFields和exportFields
			for (FieldInfo field : fieldInfoList) {
				if (field.getType() == null)
					continue;

				FieldIndexInfo info = metaInfo.addColumnByFieldsInfo(field);
				// 借助addColumnByFieldsInfo的去重功能，找出索引的和需要导出的key field;
				if (info != null) {
					if (field.isIndexFlag()) {
						fieldList.add(field);
						fieldIndexList.add(info);
					}

					if (field.isExportFlag()) {
						exportFieldList.add(field);
					}
				}
			}

			/*
			 * 数据填充 <br> 1、目前的多库多表填充，按业务只支持: <br> one to one <br> many to one
			 * <br> 2、填充参数的第一个表，应该是主表；<br>
			 * 3、关联关系，必须按顺序在参数中,如A关联B、B关联C，就不能先传A表、再传C表、最后才传B表，这样会不能按顺序填充 <br>
			 * 4、表示Join关系的字段，必须放在sql查询的前面<br>
			 */
			tabIndex = -1;
			joinIter = joinParams.iterator();
			Set<String> fieldsSet = new HashSet<String>();
			while (joinIter.hasNext()) {
				JoinParam joinParma = joinIter.next();
				++tabIndex;

				// sql查询数据集
				ResultSet rs = joinParma.getRs();
				// 需要进行数据回填的map Key名称
				String filKeyName = null;
				// 需要进行数据回填的Map数据;
				Map<String, List<GroupingArrayData>> beFillMap = null;
				// 已经回填的Map数据;
				Map<String, List<GroupingArrayData>> filledMapAlready = new HashMap<String, List<GroupingArrayData>>();
				// 本次需要创建索引的Map数据;
				// key:连接表名(joinfield_表1的ID_表2的ID);
				// value: 连接索引
				// key: 连接外键的值，如NE_CELL_ID(20123522512322)
				// value: GroupingArrayData的NE_CELL_ID == 20123522512322数据集合
				Map<String, Map<String, List<GroupingArrayData>>> keybuildMaps = new HashMap<String, Map<String, List<GroupingArrayData>>>();

				ResultSetMetaData metaData = rs.getMetaData();
				// 记录数
				int fieldCount = metaData.getColumnCount();
				// 需要回填的列序号(从1开始)
				int beginIndex = 1;
				for (int i = 1; i <= fieldCount; ++i) {
					String fieldName = metaData.getColumnName(i).toLowerCase();
					if (fieldName.indexOf(JoinKeyPrefixName) == 0) {
						// 找出要回填的数据Map和要创建连接关系的Key map;
						if (!mapKeys.containsKey(fieldName)) {
							// 创建外连接关系的Key map;
							Map<String, List<GroupingArrayData>> keyMap = new HashMap<String, List<GroupingArrayData>>();
							mapKeys.put(fieldName, keyMap);
							keybuildMaps.put(fieldName, keyMap);
						} else {
							// 找出要回填的数据Map
							filKeyName = fieldName;
							// beFillMap = mapKeys.get(fieldName);
							// 已经回填的map索引可以先删除，因为索引的name只是描述两个表之中间的关联，如Joinfield_TABLE1_TABLE2
							beFillMap = mapKeys.remove(filKeyName);
							if (beFillMap == null) {
								throw new Exception("无法回填" + filKeyName + ", 关联索引找不到，可能是参数的顺序输入的不正确。");
							}
						}

						// 如果列是join的关键字，则该列不需要填充到GroupArrayingData中;
						++beginIndex;
						continue;
					} else if (!fieldsSet.contains(fieldName)) {
						// 设置每个字段要回填到GroupingArrayData的位置信息
						fieldIndexInfos[tabIndex][i - 1] = metaInfo.getFileIndexInfo(fieldName);
						// 让已经加过的字段名，不需要重复回填
						fieldsSet.add(fieldName);
					}
				}

				while (rs.next()) {
					// 选择要加填的记录
					List<GroupingArrayData> fillRecordList = null;
					if (beFillMap == null) {
						// 如果需要填充的Map为空，则创建新记录到BlockData中
						fillRecordList = new LinkedList<GroupingArrayData>();
						GroupingArrayData record = new GroupingArrayData(metaInfo);
						// 加入到总记录列表中
						groupingArrayDataList.add(record);
						// 加入到填充列表中
						fillRecordList.add(record);
					} else {
						// 根据joinKey, 找出要回填的记录
						String joinKey = rs.getString(filKeyName);
						fillRecordList = beFillMap.get(joinKey);

						if (fillRecordList == null) {
							// ubp监控的表是一对一或多对一的关系，如果关联不到，则直接跳过，相当于inner join;
							continue;
						} else {
							// 已经回填过的,将在当前Map中删除
							beFillMap.remove(joinKey);

							// 将已经填充过的记录，记录到filledMap中
							filledMapAlready.put(joinKey, fillRecordList);
						}
					}

					// 数据填充
					Iterator<GroupingArrayData> iterRecord = fillRecordList.iterator();
					while (iterRecord.hasNext()) {
						GroupingArrayData record = iterRecord.next();
						// 链接关键字，跳过
						for (int i = beginIndex; i <= fieldCount; ++i) {
							// String fieldName=
							// metaData.getColumnName(i).toLowerCase();
							// 为NULL的是因为有一些重复的Field，此时不需要回填
							if (fieldIndexInfos[tabIndex][i - 1] == null)
								continue;
							// 从rs中读取数据内容回填到record中
							buildData(rs, record, fieldIndexInfos[tabIndex][i - 1]);
						}
					}

					// 循环已填充后的record,创建新的Key map，为方便其它便来连接自己
					if (keybuildMaps.size() > 0) {
						iterRecord = fillRecordList.iterator();
						while (iterRecord.hasNext()) {
							GroupingArrayData record = iterRecord.next();
							// 循环需要创建的外连接key map;
							Iterator<Entry<String, Map<String, List<GroupingArrayData>>>> iterBuildKey = keybuildMaps.entrySet().iterator();
							while (iterBuildKey.hasNext()) {
								Entry<String, Map<String, List<GroupingArrayData>>> entry = iterBuildKey.next();
								// 连接字段名
								String joinKeyName = entry.getKey();
								// key列表
								Map<String, List<GroupingArrayData>> keyMap = entry.getValue();

								// key索引名称
								String key = rs.getString(joinKeyName);
								List<GroupingArrayData> recordList = keyMap.get(key);
								if (recordList == null) {
									// 创建外连接列表
									recordList = new LinkedList<GroupingArrayData>();
									keyMap.put(key, recordList);
								}

								// 将当前记录加入到外连接列表中
								recordList.add(record);
							}
						}
					}
				}
			}

			// 数据依IndexKey分组
			Map<String, GroupingArrayData> groupDataMap = new HashMap<String, GroupingArrayData>();
			for (GroupingArrayData groupingArrayData : groupingArrayDataList) {
				String key = buildIndex(groupingArrayData, fieldIndexList);
				groupDataMap.put(key, groupingArrayData);
			}

			blockData.setIndexKey(getIndex(fieldList));
			blockData.setExportFieldsKey(getExportFieldsKey(exportFieldList));
			blockData.setGroupingArrayDatas(groupDataMap);

			return blockData;
		} catch (Exception e) {
			throw e;
		} finally {
			Iterator<JoinParam> joinIter = joinParams.iterator();
			while (joinIter.hasNext()) {
				JoinParam joinParma = joinIter.next();
				ResultSet rs = joinParma.getRs();
				Statement st = null;
				Connection conn = null;

				if (rs != null)
					st = rs.getStatement();

				if (st != null)
					conn = st.getConnection();

				DatabaseUtil.close(conn, st, rs);
			}
		}
	}

	/**
	 * 主键索引,多字段以-连接
	 * 
	 * @param FieldInfoList
	 * @return
	 * @throws Exception
	 */
	private String getIndex(List<FieldInfo> FieldInfoList) throws Exception {
		List<String> fieldlist = new ArrayList<String>();
		for (FieldInfo field : FieldInfoList) {
			if (field.isIndexFlag())
				fieldlist.add(field.getName());
		}
		if (fieldlist.size() == 0)
			throw new Exception("表的索引不能为空");
		return StringUtils.join(fieldlist.iterator(), MonitorConstant.KEY_SPLIT);
	}

	private String buildIndex(GroupingArrayData record, List<FieldIndexInfo> FieldInfoList) throws Exception {
		if (FieldInfoList == null || FieldInfoList.size() == 0)
			throw new Exception("表的索引不能为空");

		StringBuilder indexKey = new StringBuilder();
		for (FieldIndexInfo fieldInfo : FieldInfoList) {
			if (indexKey.length() > 0)
				indexKey.append(MonitorConstant.KEY_SPLIT);

			indexKey.append(record.getPropertyValue(fieldInfo));
		}

		return indexKey.toString();
	}

	/**
	 * 导出字段名索引,多字段以-连接
	 * 
	 * @param FieldInfoList
	 * @return
	 * @throws Exception
	 */
	private String getExportFieldsKey(List<FieldInfo> FieldInfoList) throws Exception {
		List<String> fieldlist = new ArrayList<String>();
		for (FieldInfo field : FieldInfoList) {
			if (field.isExportFlag())
				fieldlist.add(field.getName());
		}
		if (fieldlist.size() == 0)
			return "";
		return StringUtils.join(fieldlist.iterator(), MonitorConstant.KEY_SPLIT);
	}

	@Override
	public Timestamp getAvailableDataTime(Timestamp currDataTime) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}

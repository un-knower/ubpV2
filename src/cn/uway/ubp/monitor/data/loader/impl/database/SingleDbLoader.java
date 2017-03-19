package cn.uway.ubp.monitor.data.loader.impl.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.framework.util.database.ResultSetHelper;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.ConnectInfoDAO;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.DataSourceLogDAO;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;

/**
 * 单库方式的数据加载器
 * 
 * @author Chris 2014-3-11
 */
public class SingleDbLoader extends DbLoader {

	/**
	 * 查询超时时间
	 */
	private static int TIMEOUT = Configuration
			.getInteger(Configuration.DATASOURCE_MAX_LOAD_TIME) * 60;

	private static final Logger logger = LoggerFactory
			.getLogger(SingleDbLoader.class);

	public SingleDbLoader(DataSource dataSource) {
		super(dataSource);
	}

	@Override
	public BlockData load() throws Exception {
		Connection taskConn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int count = 0;
		try {
			taskConn = DbPoolManager.getConnectionForTask();
			SingleSqlBuilder sqlBuilder = new SingleSqlBuilder(taskConn, dataSource);
			String sql = sqlBuilder.build(taskConn);
			
			DbSourceInfo dbSource = dataSource.getDbSourceInfo();
			List<DbSourceTable> tableList = dbSource.getDbSourceTableList();
			DatabaseConnectionInfo databaseConnectionInfo = ConnectInfoDAO
					.getInstance().getDatabaseConnectionInfo(
							taskConn,tableList.get(0).getConnectionId());
			conn = DBPool.createDataSource(databaseConnectionInfo)
					.getConnection();

			logger.debug("数据源[{}]，数据时间[{}]，开始查询，SQL：{}", new Object[]{
					dataSource.getId(), dataSource.getDataTime(), sql});
			long loadStartTime = System.currentTimeMillis();

			pstmt = conn.prepareStatement(sql);
			pstmt.setFetchSize(100);
			pstmt.setQueryTimeout(TIMEOUT);
			rs = pstmt.executeQuery();

			long loadEndTime = System.currentTimeMillis();
			logger.debug("数据源[{}]，数据时间[{}]，完成查询，耗时{}ms", new Object[]{
					dataSource.getId(), dataSource.getDataTime(),
					(loadEndTime - loadStartTime)});

			logger.debug("数据源[{}]，数据时间[{}]，开始数据组装",
					new Object[]{dataSource.getId(), dataSource.getDataTime()});
			long assembleStartTime = System.currentTimeMillis();

			List<FieldInfo> fieldList = DataSourceDAO.getInstance()
					.getDbSourceFieldList(taskConn, dataSource.getId());

			List<FieldInfo> indexKeyList = new ArrayList<>();
			List<FieldInfo> exportFieldsList = new ArrayList<>();
			for (FieldInfo field : fieldList) {
				if (field.isIndexFlag()) {
					indexKeyList.add(field);
				}

				if (field.isExportFlag()) {
					exportFieldsList.add(field);
				}
			}

			BlockData blockData = new BlockData();
			fieldList = getFieldInfosHaveType(fieldList, rs);
			GroupingArrayDataDescriptor descriptor = new GroupingArrayDataDescriptor(
					fieldList);
			blockData.setMetaInfo(descriptor);

			Map<String, GroupingArrayData> groupingArrayDatas = new HashMap<String, GroupingArrayData>();
			blockData.setGroupingArrayDatas(groupingArrayDatas);

			blockData.setIndexKey(StringUtils.join(indexKeyList.iterator(),
					MonitorConstant.KEY_SPLIT));
			if (!exportFieldsList.isEmpty()) {
				blockData
						.setExportFieldsKey(StringUtils.join(
								exportFieldsList.iterator(),
								MonitorConstant.KEY_SPLIT));
			}
			logger.debug("数据源[{}]，数据时间[{}]，开始组装groupingArrayDatas", new Object[]{
					dataSource.getId(), dataSource.getDataTime()});
			while (rs.next()) {
				// 循环每一列，添加数据
				GroupingArrayData rawData = new GroupingArrayData(descriptor);
				for (FieldInfo field : fieldList) {
					FieldIndexInfo fieldIndexInfo = descriptor
							.getFileIndexInfo(field.getName().toLowerCase());
					if (fieldIndexInfo == null)
						continue;

					buildData(rs, rawData, fieldIndexInfo);
				}

				// 循环添加索引
				List<String> indexValueList = new ArrayList<String>();
				for (FieldInfo fieldInfoIndex : indexKeyList) {
					indexValueList.add(getIndexValue(rs, fieldInfoIndex));
				}

				// 处理完一行数据，加到数据集中，以索引为主键
				groupingArrayDatas.put(StringUtils.join(
						indexValueList.iterator(), MonitorConstant.KEY_SPLIT),
						rawData);
				count++;
			}
			logger.debug("数据源[{}]，数据时间[{}]，groupingArrayDatas组装完成", new Object[]{
					dataSource.getId(), dataSource.getDataTime()});

			long assembleEndTime = System.currentTimeMillis();
			logger.debug("数据源[{}]，数据时间[{}]，完成数据组装，共计{}条记录，耗时{}ms",
					new Object[]{dataSource.getId(), dataSource.getDataTime(),
							count, (assembleEndTime - assembleStartTime)});
			return blockData;
		} catch (SQLException e) {
			logger.error("数据源[{}]，数据时间[{}]，加载数据异常",
					new Object[]{dataSource.getId(), dataSource.getDataTime(),
							e});
			return null;
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
			DatabaseUtil.close(taskConn);
		}
	}

	@Override
	public Timestamp getAvailableDataTime(Timestamp currDataTime)
			throws SQLException {
		Timestamp dataTime = currDataTime;

		DbSourceInfo dbSource = dataSource.getDbSourceInfo();
		if (dataSource.isLogDrive()) {
			String tableName = dbSource.getTimeFieldTable();
			Connection taskConn = DbPoolManager.getConnectionForTask();
			try{
				dataTime = DataSourceLogDAO.getInstance().checkDataTime(taskConn, tableName,
						dataSource.getDataTime());
			} finally {
				DatabaseUtil.close(taskConn);
			}
		}

		return dataTime;
	}

	/**
	 * 根据数据的类型，从数据集合中获取数据
	 * 
	 * @param rs
	 * @param fieldInfo
	 * @return
	 * @throws SQLException
	 */
	private String getIndexValue(ResultSet rs, FieldInfo fieldInfo)
			throws SQLException {
		if (rs.getObject(fieldInfo.getName()) == null) {
			return "#";
		}

		switch (fieldInfo.getType()) {
			case FieldType.BYTE :
				return String.valueOf(rs.getByte(fieldInfo.getName()));
			case FieldType.INT :
				return String.valueOf(rs.getInt(fieldInfo.getName()));
			case FieldType.SHORT :
				return String.valueOf(rs.getShort(fieldInfo.getName()));
			case FieldType.LONG :
				return String.valueOf(rs.getLong(fieldInfo.getName()));
			case FieldType.FLOAT :
				return String.valueOf(rs.getFloat(fieldInfo.getName()));
			case FieldType.DOUBLE :
				return String.valueOf(rs.getDouble(fieldInfo.getName()));
			case FieldType.STRING :
				return rs.getString(fieldInfo.getName());
			case FieldType.DATE :
				return String.valueOf(rs.getDate(fieldInfo.getName()));
			default :
				throw new NullPointerException("键值" + fieldInfo.getName()
						+ "未知类型");
		}

	}

	/**
	 * 通过数据连接集来获取对应字段的数据类型
	 * 
	 * @param fieldList
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private List<FieldInfo> getFieldInfosHaveType(List<FieldInfo> fieldList,
			ResultSet rs) throws SQLException {
		for (FieldInfo field : fieldList) {
			field.setType(ResultSetHelper.getFieldType(rs, field.getName()));
		}

		return fieldList;
	}

}

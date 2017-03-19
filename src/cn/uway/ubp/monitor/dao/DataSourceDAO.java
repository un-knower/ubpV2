package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.data.model.FieldInfo;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.framework.util.net.NetUtil;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.ExField;
import cn.uway.util.entity.ViewField;

/**
 * 数据源DAO
 * 
 * @author liuchao @ 2013-6-24
 */
public class DataSourceDAO extends DAO {

	private static final DataSourceDAO DAO = new DataSourceDAO();

	private static final Logger logger = LoggerFactory
			.getLogger(DataSourceDAO.class);

	/**
	 * 本地计算机名
	 */
	private static final String HOST_NAME = NetUtil.getHostName();

	private static final String COMMON_SQL = "select t.id,t.condition,t.granularity,t.data_time,t.data_time_delay,t.ne_level,t.net_type,t.time_field_type"
			+ ", NVL(vt.table_name,t.master_table) AS master_table,NVL(f.source_name,t.time_field) AS time_field"
			+ ", t.type, t.is_log_drive"
			+ " from ubp_monitor_datasource t LEFT JOIN uway_view_info i ON lower(t.master_table)=lower(i.view_name)"
			+ " LEFT JOIN uway_view_table_relation r ON lower(r.view_name)=lower(i.view_name) AND lower(r.table_name)=lower(i.view_main_talbe)"
			+ " LEFT JOIN uway_view_table vt ON vt.id=r.table_id"
			+ " LEFT JOIN uway_view_field f ON f.table_id=vt.id AND lower(f.aliases_name)=lower(t.time_field)";

	private static final String COMMON_SQL_FIELD = "select NVL(v.id,b.table_id) AS table_id,"
			+ " CASE WHEN t.Aliases_name IS NULL THEN"
			+ " a.field_name"
			+ " ELSE"
			+ " t.source_name ||' as '|| t.Aliases_name"
			+ " END"
			+ " AS field_name"
			+ " ,is_index,is_export,"
			+ " NVL(v.connection_id,b.connection_id) AS connection_id"
			+ " from ubp_monitor_datasource_field a"
			+ " LEFT JOIN ubp_monitor_datasource_table b ON a.table_id=b.table_id"
			+ " LEFT JOIN ubp_monitor_datasource c ON c.id=b.datasource_id"
			+ " LEFT JOIN UWAY_VIEW_FIELD t ON  lower(t.view_name)=lower(b.table_name)  AND lower(t.Aliases_name)=lower(field_name)"
			+ " LEFT JOIN UWAY_VIEW_INFO i ON lower(i.view_name)=lower( b.table_name)"
			+ " LEFT JOIN uway_view_table_relation r ON lower(r.view_name)=lower(i.view_name) AND lower(r.table_name)=lower(i.view_main_talbe)"
			+ " LEFT JOIN  UWAY_VIEW_TABLE v ON v.id=r.table_id"
			+ " where c.id=?" + " order by a.order_id";

	public static DataSourceDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取所有数据源信息
	 * 
	 * @return
	 * @throws SQLException
	 * </pre>
	 */
	public Map<Long, DataSource> getAllDataSource(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL;

		Map<Long, DataSource> allDataSource;

		try {
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);

			rs = pstmt.executeQuery();

			allDataSource = new HashMap<>();

			while (rs.next()) {
				DataSource dataSource = new DataSource(rs.getInt(1),
						rs.getString(3), rs.getTimestamp(4), rs.getString(6),
						rs.getString(7), rs.getString(2), rs.getInt(5),
						rs.getString(10), rs.getString(8), rs.getString(9),
						rs.getInt(11), rs.getInt(12));
				allDataSource.put(dataSource.getId(), dataSource);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return allDataSource;
	}

	/**
	 * <pre>
	 * 获取指定数据源信息
	 * 
	 * @param id 数据源Id
	 * @return 数据源信息,数据库查不到记录则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public DataSource getDataSource(Connection conn, long id) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL + " where t.id = ?";

		DataSource dataSource = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, id);

			log(logger, conn, sql, id);

			rs = pstmt.executeQuery();

			if (rs.next()) {
				dataSource = new DataSource(rs.getInt(1), rs.getString(3),
						rs.getTimestamp(4), rs.getString(6), rs.getString(7),
						rs.getString(2), rs.getInt(5), rs.getString(10),
						rs.getString(8), rs.getString(9), rs.getInt(11),
						rs.getInt(12));
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return dataSource;
	}

	/**
	 * <pre>
	 * 获取指定数据源信息
	 * 
	 * @param id 数据源Id
	 * @return 数据源信息,数据库查不到记录则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public List<DataSource> getDataSourceWithViewInfo(Connection conn, long id)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select t.id,i.view_table_condition as condition ,t.granularity,t.data_time,t.data_time_delay,t.ne_level,t.net_type,t.time_field_type,"
				+ " t.master_table,t.time_field,t.type,t.is_log_drive"
				+ " from ubp_monitor_datasource t"
				+ " LEFT JOIN ubp_monitor_datasource_table d ON t.id = d.datasource_id"
				+ " LEFT JOIN uway_view_info i ON lower(d.table_name)=lower(i.view_name)"
				+ " WHERE t.id = ? AND i.view_table_condition IS NOT NULL";

		List<DataSource> dataSourceList = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, id);

			log(logger, conn, sql, id);

			rs = pstmt.executeQuery();
			dataSourceList = new ArrayList<>();

			while (rs.next()) {
				DataSource dataSource = new DataSource(rs.getInt(1),
						rs.getString(3), rs.getTimestamp(4), rs.getString(6),
						rs.getString(7), rs.getString(2), rs.getInt(5),
						rs.getString(9), rs.getString(8), rs.getString(10),
						rs.getInt(11), rs.getInt(12));
				dataSourceList.add(dataSource);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return dataSourceList;
	}

	/**
	 * <pre>
	 * 获取所有数据源
	 * 
	 * @return 数据源列表,如果数据库查不到记录,则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public List<DataSource> getDataSources(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		// 这里加了数据延时
		String sql = COMMON_SQL
				+ " where t.data_time + t.data_time_delay/24/60 <= sysdate"
				+ " and exists ("
				+ " select 1 from ubp_monitor_cfg_task task"
				+ " where task.datasource_id = t.id"
				+ " and task.is_used = 1"
				+ " and task.is_valid = 1"
				+ " and task.is_deleted = 0"
				+ " and(task.key_index_type in (select task_group_id from"
				+ " UBP_MONITOR_TASK_DISTRIBUTE where upper(pc_name)=upper(?)))"
				+ " )";

		List<DataSource> dataSourceList = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, HOST_NAME);

			log(logger, conn, sql, HOST_NAME);

			rs = pstmt.executeQuery();
			dataSourceList = new ArrayList<>();
			while (rs.next()) {
				DataSource dataSource = new DataSource(rs.getInt(1),
						rs.getString(3), rs.getTimestamp(4), rs.getString(6),
						rs.getString(7), rs.getString(2), rs.getInt(5),
						rs.getString(10), rs.getString(8), rs.getString(9),
						rs.getInt(11), rs.getInt(12));
				dataSourceList.add(dataSource);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return dataSourceList;
	}

	/**
	 * <pre>
	 * 修改指定的数据源时间
	 * currDataTime参数限定当前修改的数据源的时间为当前指定时间
	 * @param id 数据源Id
	 * @param nextDataTime 要修改的时间
	 * @param currDataTime 当前数据的时间，为空则不限定当前数据源时间
	 * @return 修改结果
	 * @throws SQLException
	 * 
	 * <pre>
	 */
	public boolean updateSourceDatetime(Connection conn, long id, Timestamp nextDataTime,
			Date currDataTime) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource set data_time=? where id=?";

		boolean result;

		try {
			Object[] parameters;
			if (currDataTime != null) {
				sql += " and data_time=?";
				pstmt = conn.prepareStatement(sql);

				Timestamp _currDataTime = new Timestamp(currDataTime.getTime());

				pstmt.setTimestamp(1, nextDataTime);
				pstmt.setLong(2, id);
				pstmt.setTimestamp(3, _currDataTime);

				parameters = new Object[]{nextDataTime, id, _currDataTime};
			} else {
				pstmt = conn.prepareStatement(sql);

				Timestamp _newDataTime = new Timestamp(nextDataTime.getTime());

				pstmt.setTimestamp(1, _newDataTime);
				pstmt.setLong(2, id);

				parameters = new Object[]{_newDataTime, id};
			}

			log(logger, conn, sql, parameters);

			int count = pstmt.executeUpdate();

			result = (count > 0);
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return result;
	}

	/**
	 * <pre>
	 * 获取指定数据源的表信息
	 * 
	 * @param dataSourceId 数据库Id
	 * @return 数据源表列表,数据库查不到记录,则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public List<DbSourceTable> getDbSourceTableList(Connection conn, long dataSourceId)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "SELECT NVL(t.id,d.table_id) AS table_id,nvl(r.table_name,d.table_name) AS table_name"
				+ ",nvl(t.connection_id,d.connection_id) AS connection_id,d.datasource_id,d.table_sql"
				+ " FROM ubp_monitor_datasource_table d"
				+ " LEFT JOIN uway_view_table_relation r ON lower(d.table_name)=lower(r.view_name)"
				+ " LEFT JOIN uway_view_table t ON r.table_id=t.id"
				+ " where datasource_id=?";

		Map<String, DbSourceTable> tableMap;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);

			log(logger, conn, sql, dataSourceId);

			rs = pstmt.executeQuery();

			tableMap = new HashMap<>();

			while (rs.next()) {
				DbSourceTable table = new DbSourceTable(rs.getLong(1),
						rs.getString(2), rs.getLong(3), rs.getLong(4),
						rs.getString(5));
				if (!tableMap.containsKey(table.getName())) {
					tableMap.put(table.getName(), table);
				}
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		ArrayList<DbSourceTable> list = new ArrayList<>();
		list.addAll(tableMap.values());
		return list;
	}

	/**
	 * <pre>
	 * 获取字段信息
	 * 
	 * @param id 数据源Id
	 * @return 数据源字段列表,如果数据查不到记录,则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public List<FieldInfo> getDbSourceFieldList(Connection conn , long id) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select a.table_id,field_name,is_index,is_export"
				+ " from ubp_monitor_datasource_field a,ubp_monitor_datasource_table b,ubp_monitor_datasource c"
				+ " where a.table_id = b.table_id and c.id = b.datasource_id and c.id=?"
				+ " order by a.order_id";

		List<FieldInfo> fieldList;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, id);

			log(logger, conn, sql, id);

			rs = pstmt.executeQuery();

			fieldList = new ArrayList<>();

			while (rs.next()) {
				FieldInfo field = new FieldInfo(rs.getLong(1), rs.getString(2),
						rs.getInt(3), rs.getInt(4));
				fieldList.add(field);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return fieldList;
	}

	/**
	 * <pre>
	 * 获取字段信息，以连接Id为键保存
	 * 
	 * @param id 数据源Id
	 * @return 返回以连接Id为Key,字段列表为Value的Map集合. 如果数据库查不到记录,则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public Map<Long, List<ExField>> getFieldListByConnection(Connection conn, long id)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String sql = "select a.table_id,field_name,is_index,is_export,b.connection_id"
				+ " from ubp_monitor_datasource_field a,ubp_monitor_datasource_table b,ubp_monitor_datasource c"
				+ " where a.table_id=b.table_id"
				+ " and c.id=b.datasource_id"
				+ " and c.id=?" + " order by a.order_id";

		Map<Long, List<ExField>> exFieldMap;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, id);

			log(logger, conn, sql, id);

			rs = pstmt.executeQuery();

			exFieldMap = new HashMap<>();

			while (rs.next()) {
				ExField exField = new ExField(rs.getLong(1), rs.getString(2),
						rs.getInt(3), rs.getInt(4), rs.getLong(5));
				List<ExField> fieldList = exFieldMap.get(exField
						.getConnectionId());
				if (fieldList == null) {
					fieldList = new ArrayList<>();
					exFieldMap.put(exField.getConnectionId(), fieldList);
				}

				fieldList.add(exField);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return exFieldMap;
	}

	/**
	 * <pre>
	 * 获取字段信息
	 * @param id 数据源Id
	 * @return
	 * @throws SQLException
	 * </pre>
	 */
	public List<ExField> getFieldListWihView(Connection conn, long id) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		List<ExField> exFieldList = null;

		try {
			pstmt = conn.prepareStatement(COMMON_SQL_FIELD);
			pstmt.setLong(1, id);

			log(logger, conn, COMMON_SQL_FIELD, id);

			rs = pstmt.executeQuery();

			exFieldList = new ArrayList<>();

			while (rs.next()) {
				ExField field = new ExField(rs.getLong(1), rs.getString(2),
						rs.getInt(3), rs.getInt(4), rs.getLong(5));
				exFieldList.add(field);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return exFieldList;
	}

	/**
	 * <pre>
	 * 获取字段信息，以连接Id为键保存
	 * 
	 * @param id 数据源Id
	 * @return 返回以连接Id为Key,字段列表为Value的Map集合. 如果数据库查不到记录,则返回空Map
	 * @throws SQLException
	 * </pre>
	 */
	public Map<Long, List<ExField>> getFieldInfosWihView(Connection conn, long id)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		Map<Long, List<ExField>> exFieldMap;

		try {
			pstmt = conn.prepareStatement(COMMON_SQL_FIELD);
			pstmt.setLong(1, id);

			log(logger, conn, COMMON_SQL_FIELD, id);

			rs = pstmt.executeQuery();

			exFieldMap = new HashMap<>();

			while (rs.next()) {
				ExField exField = new ExField(rs.getLong(1), rs.getString(2),
						rs.getInt(3), rs.getInt(4), rs.getLong(5));
				List<ExField> fieldList = exFieldMap.get(exField
						.getConnectionId());
				if (fieldList == null) {
					fieldList = new ArrayList<>();
					exFieldMap.put(exField.getConnectionId(), fieldList);
				}

				fieldList.add(exField);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return exFieldMap;
	}

	/**
	 * <pre>
	 * 获取多表条件替换字段情况,取得关联字段
	 * 
	 * @param id 数据源Id
	 * @return 任务索引字段列表,如果数据查不到记录,则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public List<ViewField> getViewFieldByDateSourceId(Connection conn, long id)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String sql = "SELECT lower(f.view_name||'.'||f.aliases_name ) AS as_name,lower(r.table_name||'.'||f.source_name) AS source_name"
				+ " FROM ubp_Monitor_datasource_table dt"
				+ " LEFT JOIN ubp_Monitor_datasource d ON dt.datasource_id=d.id"
				+ " LEFT JOIN uway_view_table_relation r ON r.view_name=dt.table_name"
				+ " LEFT JOIN uway_view_field f ON f.table_id=r.table_id"
				+ " WHERE dt.datasource_id=?";

		List<ViewField> fieldList;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, id);

			log(logger, conn, sql, id);

			rs = pstmt.executeQuery();

			fieldList = new ArrayList<>();

			while (rs.next()) {
				ViewField field = new ViewField(rs.getString(1),
						rs.getString(2));
				fieldList.add(field);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return fieldList;
	}

}

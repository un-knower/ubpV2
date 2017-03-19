package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.util.entity.MonitorTaskStatus;

/**
 * <pre>
 * 数据源加载状态DAO
 * 
 * @author liuchao
 * @ 2013-12-18
 * </pre>
 */
public class DataSourceStatusDAO extends DAO {

	private static final DataSourceStatusDAO DAO = new DataSourceStatusDAO();

	private static final Logger logger = LoggerFactory.getLogger(MonitorTaskStatus.class);

	public static DataSourceStatusDAO getInstance() {
		return DAO;
	}
	
	/**
	 * 查询本数据源之前的最大加载量。默认前7天最大
	 * @param dataSourceId
	 * @param dataTime
	 * @return
	 */
	public int getMaxLoadCount(Connection conn, long dataSourceId, Timestamp dataTime){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select max(load_count) from ubp_monitor_datasource_status where datasource_id=? and data_time>?-7";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);
			pstmt.setTimestamp(2, dataTime);

			log(logger, conn, sql, rs, dataSourceId, dataTime);
			
			rs = pstmt.executeQuery();
			if (rs.next()) {
				int count = rs.getInt(1);
				return count;
			}else{
				return 0;
			}
		} catch (Exception e) {
			logger.error("查询本数据源之前的最大加载量失败", e);
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 添加/更新数据源加载运行状态记录
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * </pre>
	 */
	public void addDataSourceStatus(Connection conn, long dataSourceId, Timestamp dataTime) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select count(0) from ubp_monitor_datasource_status where datasource_id=? and data_time=?";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);
			pstmt.setTimestamp(2, dataTime);

			log(logger, conn, sql, rs, dataSourceId, dataTime);
			
			rs = pstmt.executeQuery();
			if (rs.next()) {
				int count = rs.getInt(1);
				if (count == 0) {
					addDataSourceStatusForStartRun(conn, dataSourceId, dataTime);
				} else {
					updateDataSourceStatusForStartRun(conn, dataSourceId, dataTime);
				}
			}
		} catch (Exception e) {
			logger.error("查询数据源加载运行状态记录失败", e);
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}
	}

	/**
	 * <pre>
	 * 添加数据源加载运行状态记录
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * </pre>
	 */
	private void addDataSourceStatusForStartRun(Connection conn, long dataSourceId, Timestamp dataTime) {
		PreparedStatement pstmt = null;
		String sql = "insert into ubp_monitor_datasource_status(datasource_id,data_time,run_start_time) values(?,?,?)";

		try {
			Timestamp runStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);
			pstmt.setTimestamp(2, dataTime);
			pstmt.setTimestamp(3, runStartTime);

			log(logger, conn, sql, dataSourceId, dataTime, runStartTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("添加数据源加载运行开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源加载运行状态记录
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间 
	 * @return 更新成功返回true，否则返回false
	 * </pre>
	 */
	private void updateDataSourceStatusForStartRun(Connection conn, long dataSourceId, Timestamp dataTime) {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set run_start_time=?"
				+ ",load_start_time=null,load_end_time=null,load_count=null,load_fail_cause=null,store_start_time=null"
				+ ",store_end_time=null,store_fail_cause=null,run_end_time=null,run_status=null"
				+ " where datasource_id=? and data_time=?";

		try {
			Timestamp runStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, runStartTime);
			pstmt.setLong(2, dataSourceId);
			pstmt.setTimestamp(3, dataTime);

			log(logger, conn, sql, runStartTime, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源加载运行开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源加载状态ForEnd
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * @param status 状态 0失败，1成功
	 * @param cause 失败原因
	 * </pre>
	 */
	public void updateDataSourceStatusForEndRun(Connection conn, long dataSourceId, Timestamp dataTime, int status) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set run_end_time=?,run_status=? where datasource_id=? and data_time=?";

		try {
			Timestamp runEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, runEndTime);
			pstmt.setInt(2, status);
			pstmt.setLong(3, dataSourceId);
			pstmt.setTimestamp(4, dataTime);

			log(logger, conn, sql, runEndTime, status, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源加载运行完成时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源加载开始时间ForStartLoad
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * @param loadStartTime 数据加载开始时间
	 * </pre>
	 */
	public void updateDataSourceStatusForStartLoad(Connection conn, long dataSourceId, Timestamp dataTime) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set load_start_time=? where datasource_id=? and data_time=?";

		try {
			Timestamp loadStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, loadStartTime);
			pstmt.setLong(2, dataSourceId);
			pstmt.setTimestamp(3, dataTime);

			log(logger, conn, sql, loadStartTime, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源加载开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源加载结束时间ForEndLoad
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * @param loadCount 加载数据条数
	 * @param loadFailCause 加载失败原因
	 * @return 更新成功返回true，否则返回false
	 * </pre>
	 */
	public void updateDataSourceStatusForEndLoad(Connection conn, long dataSourceId, Timestamp dataTime, int loadCount, String loadFailCause) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set load_end_time=?,load_count=?,load_fail_cause=? where datasource_id=? and data_time=?";

		try {
			Timestamp loadEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, loadEndTime);
			pstmt.setInt(2, loadCount);
			pstmt.setString(3, loadFailCause);
			pstmt.setLong(4, dataSourceId);
			pstmt.setTimestamp(5, dataTime);

			log(logger, conn, sql, loadEndTime, loadCount, loadFailCause, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源加载结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源序列化开始时间ForStartStore
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * @param storeStartTime 序列化开始时间
	 * </pre>
	 */
	public void updateDataSourceStatusForStartStore(Connection conn, long dataSourceId, Timestamp dataTime) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set store_start_time=? where datasource_id=? and data_time=?";

		try {
			Timestamp storeStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, storeStartTime);
			pstmt.setLong(2, dataSourceId);
			pstmt.setTimestamp(3, dataTime);

			log(logger, conn, sql, storeStartTime, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源序列化开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源序列化结束时间ForStartStore
	 * @param dataSourceId 数据源Id
	 * @param dataTime 数据时间
	 * @param storeFailCause 序列化失败原因
	 * </pre>
	 */
	public void updateDataSourceStatusForEndStore(Connection conn, long dataSourceId, Timestamp dataTime, String storeFailCause) {
		if (!Configuration.getBoolean(Configuration.DATASOURCE_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_status set store_end_time=?,store_fail_cause=? where datasource_id=? and data_time=?";

		try {
			Timestamp storeEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, storeEndTime);
			pstmt.setString(2, storeFailCause);
			pstmt.setLong(3, dataSourceId);
			pstmt.setTimestamp(4, dataTime);

			log(logger, conn, sql, storeEndTime, storeFailCause, dataSourceId, dataTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新数据源序列化结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

}

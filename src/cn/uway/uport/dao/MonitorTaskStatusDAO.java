package cn.uway.uport.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.uport.context.ApplicationContext;
import cn.uway.uport.context.DbPoolManager;
import cn.uway.util.entity.MonitorTaskStatus;

/**
 * 监控任务状态DAO
 * 
 * @author liuchao 2013年7月5日
 */
public class MonitorTaskStatusDAO extends DAO {

	private static final MonitorTaskStatusDAO DAO = new MonitorTaskStatusDAO();

	private static final Logger logger = LoggerFactory.getLogger(MonitorTaskStatusDAO.class);
	
	private static final String COMMON_SQL = "select ts.task_id,ts.curr_monitor_time,ts.datasource_id"
			+ ",ts.expression_start_time,ts.expression_end_time,ts.expression_experience,ts.expression_fail_cause,ts.expression_event_num"
			+ ",ts.frequency_start_time,ts.frequency_end_time,ts.frequency_event_num,ts.frequency_fail_cause"
			+ ",ts.topn_start_time,ts.topn_end_time,ts.topn_event_num,ts.topn_fail_cause"
			+ ",ts.alarm_level_start_time,ts.alarm_level_end_time,ts.alarm_level_event_num,ts.alarm_level_fail_cause"
			+ ",ts.alarm_export_start_time,ts.alarm_export_end_time,ts.alarm_export_num,ts.alarm_export_fail_cause"
			+ ",ts.run_start_time,ts.run_end_time,ts.run_status"
			+ " from ubp_monitor_cfg_task t, ubp_monitor_task_status ts"
			+ " where t.task_id=ts.task_id and t.is_deleted=0";

	public static MonitorTaskStatusDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取调用者所有监控任务的状态
	 * 
	 * @param callerId 调用者Id
	 * @return 监控任务状态数组
	 * @throws SQLException
	 * </pre>
	 */
	public List<MonitorTaskStatus> getAllMonitorTaskStatus(int callerId) throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL + " and t.caller_id=? order by task_id, ts.curr_monitor_time desc";
		List<MonitorTaskStatus> statusList = new ArrayList<>();

		try {
			conn = DbPoolManager.getConnection(ApplicationContext.TASK_DATABASE);
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, callerId);

			log(logger, conn, sql, callerId);
			
			rs = pstmt.executeQuery();
			while (rs.next()) {
				MonitorTaskStatus status = new MonitorTaskStatus(rs.getInt(1), rs.getTimestamp(2), rs.getInt(3)
						, rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6), rs.getString(7), rs.getInt(8)
						, rs.getTimestamp(9), rs.getTimestamp(10), rs.getInt(11), rs.getString(12)
						, rs.getTimestamp(13), rs.getTimestamp(14), rs.getInt(15), rs.getString(16)
						, rs.getTimestamp(17), rs.getTimestamp(18), rs.getInt(19), rs.getString(20)
						, rs.getTimestamp(21), rs.getTimestamp(22), rs.getInt(23), rs.getString(24)
						, rs.getTimestamp(25), rs.getTimestamp(26), rs.getInt(27));
				statusList.add(status);
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return statusList;
	}

	/**
	 * <pre>
	 * 获取指定的监控任务状态
	 * 
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者Id
	 * @return 监控任务状态
	 * @throws SQLException
	 * </pre>
	 */
	public List<MonitorTaskStatus> getMonitorTaskStatusById(int callerId, String monitorTaskId) throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL + " and t.caller_id=? and t.monitor_task_id=? order by ts.curr_monitor_time desc";
		List<MonitorTaskStatus> statusList = new ArrayList<>();

		try {
			conn = DbPoolManager.getConnection(ApplicationContext.TASK_DATABASE);
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, callerId);
			pstmt.setString(2, monitorTaskId);

			log(logger, conn, sql, callerId, monitorTaskId);
			
			rs = pstmt.executeQuery();
			while (rs.next()) {
				MonitorTaskStatus status = new MonitorTaskStatus(rs.getInt(1), rs.getTimestamp(2), rs.getInt(3)
						, rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6), rs.getString(7), rs.getInt(8)
						, rs.getTimestamp(9), rs.getTimestamp(10), rs.getInt(11), rs.getString(12)
						, rs.getTimestamp(13), rs.getTimestamp(14), rs.getInt(15), rs.getString(16)
						, rs.getTimestamp(17), rs.getTimestamp(18), rs.getInt(19), rs.getString(20)
						, rs.getTimestamp(21), rs.getTimestamp(22), rs.getInt(23), rs.getString(24)
						, rs.getTimestamp(25), rs.getTimestamp(26), rs.getInt(27));
				statusList.add(status);
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return statusList;
	}

}

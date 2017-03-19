package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.util.entity.MonitorTaskStatus;

/**
 * <pre>
 * 监控任务状态DAO
 * 数据源加载线程添加记录
 * 数据过滤器器、表达式运算、规则运算、警告入库等更新记录数据
 * 任意一个环节失败，即设置状态(TASK_STATUS)为0
 * @author liuchao @2013-11-13
 * </pre>
 */
public class MonitorTaskStatusDAO extends DAO {

	private static final MonitorTaskStatusDAO DAO = new MonitorTaskStatusDAO();

	private static final Logger logger = LoggerFactory.getLogger(MonitorTaskStatus.class);

	public static MonitorTaskStatusDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 添加/更新监控任务运行记录
	 * 
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前监控时间
	 * @param dataSourceId 数据源Id
	 * </pre>
	 */
	public void addMonitorTaskStatus(Connection conn, long taskId, Timestamp currMonitorTime, long dataSourceId) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select count(0) from ubp_monitor_task_status where task_id=? and curr_monitor_time=?";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);
			pstmt.setTimestamp(2, currMonitorTime);

			log(logger, conn, sql, taskId, currMonitorTime);
			
			rs = pstmt.executeQuery();
			if (rs.next()) {
				int count = rs.getInt(1);
				if (count == 0) {
					addMonitorTaskStatusForStartRun(conn, taskId, currMonitorTime, dataSourceId);
				} else {
					updateMonitorTaskStatusForStartRun(conn, taskId, currMonitorTime);
				}
			}
		} catch (SQLException e) {
			logger.error("查询监控任务状态记录失败", e);
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}
	}

	/**
	 * <pre>
	 * 添加监控任务运行开始时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前监控时间
	 * @param dataSourceId 数据源Id
	 * </pre>
	 */
	private void addMonitorTaskStatusForStartRun(Connection conn, long taskId, Timestamp currMonitorTime, long dataSourceId) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "insert into ubp_monitor_task_status(task_id,curr_monitor_time,datasource_id,run_start_time) values(?,?,?,?)";

		try {
			Timestamp runStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);
			pstmt.setTimestamp(2, currMonitorTime);
			pstmt.setLong(3, dataSourceId);
			pstmt.setTimestamp(4, runStartTime);

			log(logger, conn, sql, taskId, dataSourceId, currMonitorTime, runStartTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("添加监控任务运行开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务运行开始时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前监控时间
	 * </pre>
	 */
	private void updateMonitorTaskStatusForStartRun(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set run_start_time=?"
				+ ",expression_start_time=null,expression_end_time=null,expression_experience=null,expression_fail_cause=null,expression_event_num=null"
				+ ",frequency_start_time=null,frequency_end_time=null,frequency_event_num=null,frequency_fail_cause=null"
				+ ",topn_start_time=null,topn_end_time=null,topn_event_num=null,topn_fail_cause=null"
				+ ",alarm_level_start_time=null,alarm_level_end_time=null,alarm_level_event_num=null,alarm_level_fail_cause=null"
				+ ",alarm_export_start_time=null,alarm_export_end_time=null,alarm_export_num=null,alarm_export_fail_cause=null"
				+ ",run_end_time=null,run_status=null"
				+ " where task_id=? and curr_monitor_time=?";

		try {
			Timestamp runStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, runStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, runStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新监控任务运行开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务运行结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前监控时间
	 * @param status 任务运行状态
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndRun(Connection conn, long taskId, Timestamp currMonitorTime, int status) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set run_status=?,run_end_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp runEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, status);
			pstmt.setTimestamp(2, runEndTime);
			pstmt.setLong(3, taskId);
			pstmt.setTimestamp(4, currMonitorTime);

			log(logger, conn, sql, status, runEndTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("更新监控任务运行开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务状态表达式运算开始时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * </pre>
	 */
	public void updateMonitorTaskStatusForStartExpression(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set expression_start_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp expressionStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, expressionStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, expressionStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务表达式运算开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务状态表达式运算结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param experience 表达式运算过程信息
	 * @param failCause 表达式运算失败原因
	 * @param eventNum 表达式运算产生的事件数量
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndExpression(Connection conn, long taskId, Timestamp currMonitorTime, String experience, String failCause,
			int eventNum) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set expression_end_time=?,expression_experience=?,expression_fail_cause=?,expression_event_num=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp expressionEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, expressionEndTime);
			pstmt.setString(2, experience);
			pstmt.setString(3, failCause);
			pstmt.setInt(4, eventNum);
			pstmt.setLong(5, taskId);
			pstmt.setTimestamp(6, currMonitorTime);

			log(logger, conn, sql, expressionEndTime, experience, failCause, eventNum, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务表达式运算结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务状态频次运算开始时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * </pre>
	 */
	public void updateMonitorTaskStatusForStartFrequency(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set frequency_start_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp frequencyStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, frequencyStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, frequencyStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务频次运算开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务状态频次运算结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前监控时间
	 * @param eventNum 事件数量
	 * @param failCause 频次运算失败原因
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndFrequency(Connection conn, long taskId, Timestamp currMonitorTime, int eventNum, String failCause) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set frequency_end_time=?,frequency_event_num=?,frequency_fail_cause=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp frequencyEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, frequencyEndTime);
			pstmt.setInt(2, eventNum);
			pstmt.setString(3, failCause);
			pstmt.setLong(4, taskId);
			pstmt.setTimestamp(5, currMonitorTime);

			log(logger, conn, sql, frequencyEndTime, eventNum, failCause, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务频次运算结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务TOPN规则运算开始时间
	 * 
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param startRuleAlarmDate 规则运算开始时间
	 * </pre>
	 */
	public void updateMonitorTaskStatusForStartTopN(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set topn_start_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp topnStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, topnStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, topnStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务TOPN规则运算开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务TOPN规则运算结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param eventNum 事件数量
	 * @param failCause TopN运算失败原因
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndTopN(Connection conn, long taskId, Timestamp currMonitorTime, int eventNum, String failCause) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set topn_end_time=?,topn_event_num=?,topn_fail_cause=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp topnEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, topnEndTime);
			pstmt.setInt(2, eventNum);
			pstmt.setString(3, failCause);
			pstmt.setLong(4, taskId);
			pstmt.setTimestamp(5, currMonitorTime);

			log(logger, conn, sql, topnEndTime, eventNum, failCause, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务TOPN规则运算结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务告警级别规则运算开始时间
	 * 
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param startRuleAlarmDate 规则运算开始时间
	 * </pre>
	 */
	public void updateMonitorTaskStatusForStartAlarmLevel(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set alarm_level_start_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp alarmLevelStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, alarmLevelStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, alarmLevelStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务告警级别规则运算开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务告警级别规则运算结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param eventNum 事件数量
	 * @param failCause 告警级别规则运算失败原因
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndAlarmLevel(Connection conn, long taskId, Timestamp currMonitorTime, int eventNum, String failCause) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set alarm_level_end_time=?,alarm_level_event_num=?,alarm_level_fail_cause=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp alarmLevelEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, alarmLevelEndTime);
			pstmt.setInt(2, eventNum);
			pstmt.setString(3, failCause);
			pstmt.setLong(4, taskId);
			pstmt.setTimestamp(5, currMonitorTime);

			log(logger, conn, sql, alarmLevelEndTime, eventNum, failCause, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务告警级别规则运算结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务告警输出开始时间
	 * 
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param startRuleAlarmDate 规则运算开始时间
	 * </pre>
	 */
	public void updateMonitorTaskStatusForStartAlarmExport(Connection conn, long taskId, Timestamp currMonitorTime) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set alarm_export_start_time=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp alarmExportStartTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, alarmExportStartTime);
			pstmt.setLong(2, taskId);
			pstmt.setTimestamp(3, currMonitorTime);

			log(logger, conn, sql, alarmExportStartTime, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务告警输出开始时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新监控任务告警输出结束时间
	 * @param taskId 任务Id
	 * @param currMonitorTime 当前数据时间
	 * @param num 输出事件数量
	 * @param failCause 告警级别规则运算失败原因
	 * </pre>
	 */
	public void updateMonitorTaskStatusForEndAlarmExport(Connection conn, long taskId, Timestamp currMonitorTime, int num, String failCause) {
		if (!Configuration.getBoolean(Configuration.TASK_ENABLE_DEBUG))
			return;

		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_task_status set alarm_export_end_time=?,alarm_export_num=?,alarm_export_fail_cause=? where task_id=? and curr_monitor_time=?";

		try {
			Timestamp alarmExportEndTime = new Timestamp(System.currentTimeMillis());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, alarmExportEndTime);
			pstmt.setInt(2, num);
			pstmt.setString(3, failCause);
			pstmt.setLong(4, taskId);
			pstmt.setTimestamp(5, currMonitorTime);

			log(logger, conn, sql, alarmExportEndTime, num, failCause, taskId, currMonitorTime);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("更新监控任务告警输出结束时间失败", e);
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

}

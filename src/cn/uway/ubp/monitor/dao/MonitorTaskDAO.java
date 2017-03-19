package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.framework.util.net.NetUtil;
import cn.uway.util.entity.MonitorTask;

/**
 * 监控任务操作DAO
 * 
 * @author liuchao @ 2013-11-21
 */
public class MonitorTaskDAO extends DAO {

	private static final MonitorTaskDAO DAO = new MonitorTaskDAO();

	/**
	 * 本地计算机名
	 */
	private static final String HOST_NAME = NetUtil.getHostName();

	// 查全表数据
	private static final String COMMON_SQL = "SELECT T.TASK_ID,T.TASK_NAME,T.TASK_DESCRIPTION,T.MONITOR_TASK_ID,T.CALLER_ID,T.IS_USED,T.IS_DELETED"
			+ ",T.IS_PERIOD,T.PERIOD_NUM,T.PERIOD_UNIT,T.DATASOURCE_ID,T.CURR_MONITOR_TIME,T.END_MONITOR_TIME,T.GROUP_ID,T.PC_NAME,T.CITY_ID"
			+ ",T.IS_VALID,T.PI_NAME,T.PI_EXPR_DESCRIPTION,T.FILTER,T.KEY_INDEX_TYPE,T.T_ID,T.IS_ALARM_CLEAR,T.MONITOR_FIELD,T.EXPRESSION_INFO"
			+ ",T.PERIOD_INFO,T.TOP_INFO,T.ALARM_LEVEL_INFO,T.RULE_DESCRIPTION,T.MEMO"
			+ " FROM UBP_MONITOR_CFG_TASK T";

	private static final Logger logger = LoggerFactory
			.getLogger(MonitorTaskDAO.class);

	public static MonitorTaskDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 查找给定数据源对应的所有监控任务
	 * 1、先尝试查找当前计算机名对应的任务列表.如果找到则返回
	 * 2、如果1没找到任务，则不带机器名条件查询任务
	 * 3、如果都没有找到,则返回空的List
	 * 
	 * @param dataSourceId 数据源Id
	 * @return 给定数据源对应的所有监控任务
	 * @throws SQLException
	 * </pre>
	 */
	public List<MonitorTask> getTasks(Connection conn, long dataSourceId) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL
				+ " WHERE T.DATASOURCE_ID=? AND T.IS_DELETED=0 AND T.IS_VALID=1 AND T.IS_USED=1"
				+ " and(t.key_index_type in (select task_group_id from UBP_MONITOR_TASK_DISTRIBUTE where upper(pc_name)=upper(?)))";

		List<MonitorTask> monitorTaskList;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);
			pstmt.setString(2, HOST_NAME);

			log(logger, conn, sql, rs, dataSourceId, HOST_NAME);

			rs = pstmt.executeQuery();

			monitorTaskList = new ArrayList<>();

			while (rs.next()) {
				MonitorTask task;
				try {
					task = new MonitorTask(rs.getLong(1), rs.getString(2),
							rs.getString(3), rs.getString(4), rs.getInt(5),
							rs.getInt(6), rs.getInt(7), rs.getInt(8),
							rs.getInt(9), rs.getString(10), rs.getLong(11),
							rs.getTimestamp(12), rs.getTimestamp(13),
							rs.getInt(14), rs.getString(15), rs.getInt(16),
							rs.getInt(17), rs.getString(18), rs.getString(19),
							rs.getString(20), rs.getInt(21), rs.getInt(22),
							rs.getInt(23), rs.getString(24), rs.getString(25),
							rs.getString(26), rs.getString(27),
							rs.getString(28), rs.getString(29),
							rs.getString(30));
					monitorTaskList.add(task);
					
					// Chris，2015.01.23，应前端要求，当传递的表达式为空值任务不运行。PS：这样做极不合理！
					if (task.getExpression() == null)
						continue;
				} catch (Exception e) {
					logger.error("解析监控任务失败", e);

					// 将当前任务设置为无效状态
					try {
						setTaskNotValid(conn, rs.getLong(1));
					} catch (Exception ex) {
						logger.error("设置任务无效状态失败", ex);
					}

					continue;
				}
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return monitorTaskList;
	}

	/**
	 * <pre>
	 * 1 在UBP_MONITOR_TASK_DISTRIBUTE 查找本地计算机配置的任务
	 * 2无此表或无数据程序不能启动，退出 
	 * @return  List<MonitorTask> 执行服务器对应的所有监控任务
	 * @throws SQLException 查找监控任务失败
	 * @author 魏伟
	 * @2014-7-10
	 * </pre>
	 */
	public List<MonitorTask> getExpressionTasksByTaskDistribute(Connection conn)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		StringBuilder sqlSB = new StringBuilder(COMMON_SQL);
		sqlSB.append(
				", UBP_MONITOR_DATASOURCE D where t.datasource_id = d.id and t.curr_monitor_time < d.data_time")
				.append(" and t.is_deleted = 0 and t.is_used = 1 and t.is_valid = 1")
				.append(" and (t.end_monitor_time is null or t.end_monitor_time >= t.curr_monitor_time)")
				.append(" and(t.key_index_type in (select task_group_id from UBP_MONITOR_TASK_DISTRIBUTE where upper(pc_name)=upper(?)))");

		List<MonitorTask> monitorTaskList;
		String sql = sqlSB.toString();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, HOST_NAME);
			log(logger, conn, sql, rs, HOST_NAME);
			rs = pstmt.executeQuery();
			monitorTaskList = new ArrayList<>();
			while (rs.next()) {
				MonitorTask task;
				long taskId = rs.getLong(1);
				try {
					task = new MonitorTask(taskId, rs.getString(2),
							rs.getString(3), rs.getString(4), rs.getInt(5),
							rs.getInt(6), rs.getInt(7), rs.getInt(8),
							rs.getInt(9), rs.getString(10), rs.getLong(11),
							rs.getTimestamp(12), rs.getTimestamp(13),
							rs.getInt(14), rs.getString(15), rs.getInt(16),
							rs.getInt(17), rs.getString(18), rs.getString(19),
							rs.getString(20), rs.getInt(21), rs.getInt(22),
							rs.getInt(23), rs.getString(24), rs.getString(25),
							rs.getString(26), rs.getString(27),
							rs.getString(28), rs.getString(29),
							rs.getString(30));
					monitorTaskList.add(task);
				} catch (Exception e) {
					logger.error("解析监控任务{}失败", taskId, e);

					// 将当前任务设置为无效状态
					try {
						setTaskNotValid(conn, taskId);
					} catch (Exception ex) {
						logger.error("设置任务{}无效状态失败", taskId, ex);
					}
					continue;
				}
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return monitorTaskList;

	}

	/**
	 * <pre>
	 * 查找需要进行指标运算的监控任务
	 * 1、先尝试查找当前计算机名对应的任务列表.如果找到则返回
	 * 2、如果1没找到任务，则不带机器名条件查询任务
	 * 3、如果都没有找到,则返回空的List
	 * @return List<MonitorTask> 给定数据源对应的所有监控任务
	 * @throws SQLException 查找监控任务失败
	 * @deprecated
	 * </pre>
	 */
	public List<MonitorTask> getExpressionTasks(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = COMMON_SQL
				+ ", UBP_MONITOR_DATASOURCE D"
				+ " where t.datasource_id = d.id"
				+ " and t.curr_monitor_time < d.data_time"
				+ " and t.is_deleted = 0"
				+ " and t.is_used = 1"
				+ " and t.is_valid = 1"
				+ " and (t.end_monitor_time is null or t.end_monitor_time >= t.curr_monitor_time)"
				+ " and (t.pc_name=? or "
				+ " (t.pc_name is null and not exists ("
				+ " select task.task_id" + " from ubp_monitor_cfg_task task"
				+ " where task.pc_name=?" + " )))";

		List<MonitorTask> monitorTaskList;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, HOST_NAME);
			pstmt.setString(2, HOST_NAME);

			log(logger, conn, sql, rs, HOST_NAME, HOST_NAME);

			rs = pstmt.executeQuery();

			monitorTaskList = new ArrayList<>();

			while (rs.next()) {
				MonitorTask task;
				long taskId = rs.getLong(1);
				try {
					task = new MonitorTask(taskId, rs.getString(2),
							rs.getString(3), rs.getString(4), rs.getInt(5),
							rs.getInt(6), rs.getInt(7), rs.getInt(8),
							rs.getInt(9), rs.getString(10), rs.getLong(11),
							rs.getTimestamp(12), rs.getTimestamp(13),
							rs.getInt(14), rs.getString(15), rs.getInt(16),
							rs.getInt(17), rs.getString(18), rs.getString(19),
							rs.getString(20), rs.getInt(21), rs.getInt(22),
							rs.getInt(23), rs.getString(24), rs.getString(25),
							rs.getString(26), rs.getString(27),
							rs.getString(28), rs.getString(29),
							rs.getString(30));
					monitorTaskList.add(task);
				} catch (Exception e) {
					logger.error("解析监控任务{}失败", taskId, e);

					// 将当前任务设置为无效状态
					try {
						setTaskNotValid(conn, taskId);
					} catch (Exception ex) {
						logger.error("设置任务{}无效状态失败", taskId, ex);
					}

					continue;
				}
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return monitorTaskList;
	}

	/**
	 * <pre>
	 * 更新任务监控时间为下次监控时间
	 * 
	 * @param taskId 监控任务Id
	 * @return true为修改成功，否则返回false
	 * @throws SQLException
	 * </pre>
	 */
	public boolean markNextMonitorTime(Connection conn, long taskId, Date nextMonitorTime)
			throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_cfg_task set curr_monitor_time=? where task_id=?";

		int count;

		try {
			Timestamp currMonitorTime = new Timestamp(nextMonitorTime.getTime());

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, currMonitorTime);
			pstmt.setLong(2, taskId);

			log(logger, conn, sql, currMonitorTime, taskId);

			count = pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return count == 1;
	}

	/**
	 * <pre>
	 * 设置任务为无效状态
	 * @param taskId
	 * @return true为修改成功，否则返回false
	 * @throws SQLException
	 * </pre>
	 */
	public boolean setTaskNotValid(Connection conn, long taskId) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_cfg_task set is_valid=0 where task_id=?";

		int count;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);

			log(logger, conn, sql, taskId);

			count = pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return count == 1;
	}

	/**
	 * <pre>
	 * 需要保证每次都从数据库查询 规则信息可能有人工修改的可能 尽量避免重新启动程序
	 * 
	 * @param taskId
	 * @return MonitorRule
	 * @throws SQLException
	 * </pre>
	 */
	public List<String> getTaskField(Connection conn, long taskId) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select field_Name from UBP_MONITOR_TASK_FIELD where task_id=?";

		List<String> fieldNameList = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);

			log(logger, conn, sql, rs, taskId);

			rs = pstmt.executeQuery();
			fieldNameList = new ArrayList<>();

			while (rs.next()) {
				fieldNameList.add(rs.getString(1));
			}
		} catch (Exception e) {
			throw new Exception("获取监控任务字段失败：（" + e.getMessage() + "）", e);
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return fieldNameList;
	}

	/**
	 * <pre>
	 * 删除告警清除任务
	 * 仅当正常任务被删除 或 正常任务监控时间已过期时
	 * 
	 * @param taskId 任务Id
	 * @return true为删除成功，否则返回false
	 * @throws SQLException 删除失败
	 * </pre>
	 */
	public boolean del(Connection conn, long taskId) throws SQLException {
		PreparedStatement pstmt = null;
		// 需要判断正常任务是否已被删除 或 正常任务监控时间已过期
		String sql = " update ubp_monitor_cfg_task set is_deleted = 1 where is_deleted = 0 and is_alarm_clear = 1 and task_id =? and exists "
				+ " (select 1 from ubp_monitor_cfg_task t  where (is_deleted = 1 or end_monitor_time < sysdate) and is_alarm_clear = 0 "
				+ " and end_monitor_time is not null "
				+ " and monitor_task_id =(select monitor_task_id from ubp_monitor_cfg_task where task_id = ?)) ";
		int affectedRows = 0;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);
			pstmt.setLong(2, taskId);

			log(logger, conn, sql, taskId);

			affectedRows = pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return affectedRows > 0;
	}

	/**
	 * 判断一个taskId下的monitorTaskId下是否还有可以跑的任务 BUG UBP-86
	 * 
	 * @param taskId
	 * @return
	 * @throws SQLException
	 */
	public boolean checkTheMonitorHasRunningTask(Connection conn, long taskId)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select task_id from ubp_monitor_cfg_task  uct where  monitor_task_id =(select monitor_task_id from ubp_monitor_cfg_task where task_id = ?) and is_alarm_clear = 0  and  is_deleted = 0  and is_used = 1 and is_valid = 1";
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, taskId);
			log(logger, conn, sql, taskId);
			rs = pstmt.executeQuery();
			// 如果没有数据集时返回false, 有返回true
			boolean result = rs.next();
			return result;
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

	}

}

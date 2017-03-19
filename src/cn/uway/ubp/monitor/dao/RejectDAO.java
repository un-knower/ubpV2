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
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.util.entity.ReplyTask;

public class RejectDAO extends DAO {

	private static final Logger logger = LoggerFactory
			.getLogger(RejectDAO.class);

	private static final RejectDAO DAO = new RejectDAO();

	private RejectDAO() {
	}

	public static RejectDAO getInstance() {
		return DAO;
	}

	/**
	 * 在 v_mod_alarm_process_nocancel 视图中 获取有回复时间的网元
	 * 
	 * @param monitorTaskId
	 * @param alarmTime
	 * @return
	 * @throws SQLException
	 */
	public List<ReplyTask> getReplyTaskList(Connection conn, String monitorTaskId,
			Timestamp alarmTime) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select  ne_sys_id,replyTime  from v_mod_alarm_process_nocancel where monitor_task_id=? and replyTime is not null order by replyTime";

		List<ReplyTask> replyTaskList = new ArrayList<ReplyTask>();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				replyTaskList.add(new ReplyTask(rs.getString(1), rs
						.getTimestamp(2)));
			}

			log(logger, conn, sql, rs, monitorTaskId, alarmTime);
		} catch (Exception e) {
			err(logger, conn, sql, e, monitorTaskId, alarmTime);

			throw e;
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return replyTaskList;

	}
	
	/**
	 * 在 v_mod_alarm_process_nocancel 视图中 获取有回复时间的网元
	 * 
	 * @param monitorTaskId
	 * @param alarmTime
	 * @return
	 * @throws SQLException
	 */
	public Map<String, Date> getNeSysReplyTime(Connection conn, String monitorTaskId,
			Timestamp alarmTime) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select  ne_sys_id,trunc(replyTime+1) replyTime from v_mod_alarm_process_nocancel where monitor_task_id=? and replyTime is not null order by replyTime";

		Map<String, Date> nesysReplayTimeMap = new HashMap<String, Date>();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				nesysReplayTimeMap.put(rs.getString(1), rs.getTimestamp(2));
			}

			log(logger, conn, sql, rs, monitorTaskId, alarmTime);
		} catch (Exception e) {
			err(logger, conn, sql, e, monitorTaskId, alarmTime);

			throw e;
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return nesysReplayTimeMap;

	}
}

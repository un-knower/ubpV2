package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;

/**
 * 未闭环告警DAO
 * 
 * @author liuchao @ 2013-8-11
 */
public class AlarmClearDAO extends DAO {

	private static final AlarmClearDAO DAO = new AlarmClearDAO();

	private static final Logger logger = LoggerFactory
			.getLogger(AlarmClearDAO.class);

	public static AlarmClearDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 查询指定监控任务未闭环告警的網元
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param alarmTime 告警时间
	 * @return 网元列表
	 * @throws SQLException
	 * </pre>
	 */
	public List<String> getAlarmClearIdList(Connection conn,String monitorTaskId,
			Timestamp alarmTime) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select distinct (ne_sys_id) from v_mod_alarm_process_nocancel where monitor_task_id=? and alarm_time<=?";

		List<String> neSysIdList = new ArrayList<>();

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setTimestamp(2, alarmTime);

			log(logger, conn, sql, rs, monitorTaskId, alarmTime);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				neSysIdList.add(rs.getString(1));
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return neSysIdList;
	}

}

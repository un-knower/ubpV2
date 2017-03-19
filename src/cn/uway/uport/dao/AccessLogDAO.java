package cn.uway.uport.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.uport.context.ApplicationContext;
import cn.uway.uport.context.DbPoolManager;

/**
 * 访问日志DAO
 * 
 * @author liuchao 2013年7月4日
 */
public class AccessLogDAO extends DAO {

	private static final AccessLogDAO DAO = new AccessLogDAO();

	private static final Logger logger = LoggerFactory.getLogger(AccessLogDAO.class);

	public static AccessLogDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 添加访问日志
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param callerId 调用者Id
	 * @param accessIp 请求IP
	 * @param requestContent 请求内容
	 * @param requestMethod 请求方法
	 * @param responseContent 响应内容
	 * @param responseStatus 响应状态
	 * </pre>
	 */
	@Deprecated
	public void add(String monitorTaskId, int callerId, String accessIp, String requestContent, String requestMethod
			, String responseContent, int responseStatus) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "insert into ubp_interface_access_log(log_id, monit_task_id, caller_id, request_time, access_ip, request_content, request_method, response_content, response_status) values(ubp_interface_access_log_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?)";

		try {
			conn = DbPoolManager.getConnection(ApplicationContext.TASK_DATABASE);

			Timestamp requestTime = new Timestamp(System.currentTimeMillis());
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setInt(2, callerId);
			pstmt.setTimestamp(3, requestTime);
			pstmt.setString(4, accessIp);
			pstmt.setString(5, requestContent);
			pstmt.setString(6, requestMethod);
			pstmt.setString(7, responseContent);
			pstmt.setInt(8, responseStatus);
			
			log(logger, conn, sql, new Object[]{monitorTaskId, callerId, requestTime, accessIp, requestContent, requestMethod, responseContent, responseStatus});
			
			pstmt.executeUpdate();
		} catch (Exception e) {
			logger.error("添加接口访问日志失败", e);
		} finally {
			DatabaseUtil.close(pstmt, conn);
		}
	}

	/**
	 * <pre>
	 * 添加访问日志
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param callerId 调用者Id
	 * @param accessIp 请求IP
	 * @param requestContent 请求内容
	 * @param requestMethod 请求方法
	 * @return 添加失败返回-1，否则返回id值
	 * </pre>
	 */
	public long add(String monitorTaskId, int callerId, String accessIp, String requestContent, String requestMethod) {
		Connection conn = null;
		PreparedStatement pstmtForSeq = null;
		PreparedStatement pstmtForAdd = null;
		ResultSet rs = null;
		String sqlForSeq = "select ubp_interface_access_log_seq.nextval from dual";
		String sqlForAdd = "insert into ubp_interface_access_log(log_id, monit_task_id, caller_id, request_time, access_ip, request_content, request_method, response_content, response_status) values(?, ?, ?, ?, ?, ?, ?, null, null)";
		long id = -1;

		try {
			conn = DbPoolManager.getConnection(ApplicationContext.TASK_DATABASE);

			// STEP1：取sequence
			pstmtForSeq = conn.prepareStatement(sqlForSeq);
			
			log(logger, conn, sqlForSeq);
			
			rs = pstmtForSeq.executeQuery();
			rs.next();
			id = rs.getLong(1);

			// STEP2：添加访问日志
			Timestamp requestTime = new Timestamp(System.currentTimeMillis());
			pstmtForAdd = conn.prepareStatement(sqlForAdd);
			pstmtForAdd.setLong(1, id);
			pstmtForAdd.setString(2, monitorTaskId);
			pstmtForAdd.setInt(3, callerId);
			pstmtForAdd.setTimestamp(4, requestTime);
			pstmtForAdd.setString(5, accessIp);
			pstmtForAdd.setString(6, requestContent);
			pstmtForAdd.setString(7, requestMethod);
			
			log(logger, conn, sqlForAdd, new Object[]{id, monitorTaskId, callerId, requestTime, accessIp, requestContent, requestMethod});
			
			pstmtForAdd.executeUpdate();
		} catch (Exception e) {
			logger.error("添加接口访问日志失败", e);
		} finally {
			DatabaseUtil.close(pstmtForAdd);
			DatabaseUtil.close(conn, pstmtForSeq, rs);
		}

		return id;
	}
	
	/**
	 * <pre>
	 * 更新访问日志（关于响应的信息）
	 * @param responseContent 响应内容
	 * @param responseStatus 响应状态
	 * @return 更新成功则返回true，否则返回false
	 * </pre>
	 */
	public boolean update(long id, String responseContent, int responseStatus) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "update ubp_interface_access_log set response_content=?, response_status=? where log_id=?";
		boolean result = false;

		try {
			conn = DbPoolManager.getConnection(ApplicationContext.TASK_DATABASE);

			// 更新响应内容
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, responseContent);
			pstmt.setInt(2, responseStatus);
			pstmt.setLong(3, id);
			
			log(logger, conn, sql, id, responseContent, responseStatus);
			
			pstmt.executeUpdate();

			result = true;
		} catch (Exception e) {
			logger.error("更新接口访问日志失败", e);
		} finally {
			DatabaseUtil.close(pstmt, conn);
		}

		return result;
	}

}

package cn.uway.common;

import java.sql.Connection;
import java.sql.ResultSet;

import org.slf4j.Logger;

/**
 * <pre>
 * DAO父类，模板化日志记录
 * @author liuchao
 * @ 2014年4月24日
 * </pre>
 */
public class DAO {

	/**
	 * <pre>
	 * DAO层日志公用方法
	 * 
	 * @param logger
	 * @param conn
	 * @param preparing
	 * @param parameters
	 * </pre>
	 */
	public void log(Logger logger, Connection conn, String preparing,
			Object... parameters) {
		String content = buildLog(conn, preparing, null, parameters);
		logger.debug(content);
	}

	/**
	 * <pre>
	 * DAO层日志公用方法
	 * 
	 * @param logger
	 * @param conn
	 * @param preparing
	 * @param rs
	 * @param parameters
	 * </pre>
	 */
	public void log(Logger logger, Connection conn, String preparing,
			ResultSet rs, Object... parameters) {
		String content;

		if (logger.isDebugEnabled()) {
			content = buildLog(conn, preparing, null, parameters);
			logger.debug(content);
		} else if (logger.isTraceEnabled()) {
			content = buildLog(conn, preparing, rs, parameters);
			logger.trace(content);
		}
	}

	/**
	 * <pre>
	 * DAO层错误日志公用方法
	 * @param logger
	 * @param conn
	 * @param preparing
	 * @param parameters
	 * @param e
	 * </pre>
	 */
	public void err(Logger logger, Connection conn, String preparing,
			Exception e, Object... parameters) {
		String content = buildLog(conn, preparing, null, parameters);
		logger.error(content, e);
	}

	/**
	 * <pre>
	 * 构造日志文本
	 * 
	 * @param conn 连接对象
	 * @param preparing 执行的SQL语句
	 * @param rs 结果集
	 * @param parameters 执行参数
	 * @return
	 * </pre>
	 */
	private String buildLog(Connection conn, String preparing, ResultSet rs,
			Object... parameters) {
		StringBuilder logInfo = new StringBuilder();
		logInfo.append("Connection:").append(conn).append("\n");
		logInfo.append("Preparing:").append(preparing).append("\n");
		logInfo.append("Parameters:");
		for (Object obj : parameters) {
			if (obj != null)
				logInfo.append(obj).append("(")
						.append(obj.getClass().getSimpleName()).append(")")
						.append(", ");
			else
				logInfo.append("Null").append(",");
		}

		/*
		 * if (rs != null) { try { ResultSetMetaData rsmd = rs.getMetaData();
		 * int columnCount = rsmd.getColumnCount(); for (int i=0; i<columnCount;
		 * i++) { logInfo.append(rsmd.getColumnName(i)).append("\t"); }
		 * 
		 * logInfo.append("\n");
		 * 
		 * rs.beforeFirst(); while (rs.next()) { logInfo.append(); }
		 * 
		 * rs.beforeFirst(); } catch (Exception e) {
		 * 
		 * } }
		 */

		return logInfo.toString();
	}

}

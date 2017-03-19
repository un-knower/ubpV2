package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.connection.DatabaseConnectionInfo;
import cn.uway.framework.util.database.DatabaseUtil;

/**
 * 连接配置信息查询DAO
 * 
 * @author liuchao
 * @ 2013-6-24
 */
public class ConnectInfoDAO extends DAO {

	private static final ConnectInfoDAO DAO = new ConnectInfoDAO();

	private static final Logger logger = LoggerFactory.getLogger(ConnectInfoDAO.class);

	public static ConnectInfoDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取连接信息
	 * 
	 * @param connectId 连接Id
	 * @return 返回连接信息，查不到记录则返回null
	 * @throws SQLException
	 * </pre>
	 */
	public DatabaseConnectionInfo getDatabaseConnectionInfo(Connection conn, long connectId) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select c.id,c.user_name,c.user_pwd,d.driver,d.url,d.max_active,d.max_idle,d.max_wait"
				+ " from uway_cfg_connection c left join uway_cfg_connection_db d on c.conn_relate_id=d.id" + " where c.id=?";

		DatabaseConnectionInfo connectionInfo = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, connectId);

			log(logger, conn, sql, connectId);
			
			rs = pstmt.executeQuery();

			if (rs.next()) {
				connectionInfo = new DatabaseConnectionInfo();
				connectionInfo.setId(rs.getInt(1));
				connectionInfo.setUserName(rs.getString(2));
				connectionInfo.setUserPwd(rs.getString(3));
				connectionInfo.setDbDriver(rs.getString(4));
				connectionInfo.setDbUrl(rs.getString(5));
				connectionInfo.setMaxActive(rs.getInt(6));
				connectionInfo.setMaxIdle(rs.getInt(7));
				connectionInfo.setMaxWait(rs.getInt(8));
			}else{
				logger.debug("找不到数据源连接信息--查询数据库：{}；connectId：{}", new Object[]{conn.toString(),connectId});
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return connectionInfo;
	}

}

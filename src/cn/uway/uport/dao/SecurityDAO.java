package cn.uway.uport.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.uport.context.DbPoolManager;
import cn.uway.uport.context.SecurityValidator;

/**
 * 安全校验DAO
 * 
 * @author liuchao 2013年7月4日
 */
public class SecurityDAO extends DAO {

	private static final SecurityDAO DAO = new SecurityDAO();

	private static final Logger logger = LoggerFactory.getLogger(SecurityDAO.class);

	public static SecurityDAO getInstance() {
		return DAO;
	}

	/**
	 * 载入安全校验信息
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Set<String> loadSecurityInfo() throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select caller_id, called_id, user_name, user_password from uway_interface_security";
		Set<String> securityInfos = new HashSet<String>();

		try {
			conn = DbPoolManager.getConnectionForSecurity();
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();

			// 如果加载到安全信息 则将安全信息组装成一个字符串.放入到一个set中，用于缓存
			while (rs.next()) {
				int callerId = rs.getInt(1);
				int calledId = rs.getInt(2);
				String userName = rs.getString(3);
				String password = rs.getString(4);
				String keyInfo = SecurityValidator.createKey(callerId, calledId, userName, password);
				securityInfos.add(keyInfo);
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return securityInfos;
	}

}

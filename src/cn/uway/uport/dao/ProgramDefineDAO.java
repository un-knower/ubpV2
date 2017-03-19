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
import cn.uway.uport.context.DbPoolManager;
import cn.uway.util.entity.ProgramDefine;

/**
 * 应用程序定义DAO 添加了缓存,本类暂无使用
 * 
 * @author liuchao 2013年7月4日
 */
public class ProgramDefineDAO extends DAO {

	private static final ProgramDefineDAO DAO = new ProgramDefineDAO();

	private static final Logger logger = LoggerFactory.getLogger(ProgramDefineDAO.class);

	public static ProgramDefineDAO getInstance() {
		return DAO;
	}

	/**
	 * 获取所有应用程序登记情况
	 * 
	 * @return
	 * @throws SQLException
	 */
	public List<ProgramDefine> loadAllProgramDefine() throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select program_id,program_name,program_version from uway_program_define";
		List<ProgramDefine> pdList = new ArrayList<>();

		try {
			conn = DbPoolManager.getConnectionForSecurity();
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			while (rs.next()) {
				ProgramDefine pd = new ProgramDefine(rs.getInt(1), rs.getString(2), rs.getString(3));
				pdList.add(pd);
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return pdList;
	}

}

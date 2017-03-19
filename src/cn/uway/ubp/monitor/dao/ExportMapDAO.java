package cn.uway.ubp.monitor.dao;

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
import cn.uway.util.entity.ExportMap;

/**
 * 映射字段表DAO
 * TODO 可缓存
 * 
 * @author liuchao
 * @ 2013-6-25
 */
public class ExportMapDAO extends DAO {

	private static final ExportMapDAO DAO = new ExportMapDAO();

	private static final Logger logger = LoggerFactory.getLogger(ExportMapDAO.class);

	public static ExportMapDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取映射字段表信息
	 * 
	 * @return 
	 * @throws SQLException
	 * </pre>
	 */
	public List<ExportMap> getExportMaps(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "SELECT export_field,map_field FROM ubp_Monitor_ExportMap";

		List<ExportMap> exportMapList = null;

		try {
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			exportMapList = new ArrayList<>();

			while (rs.next()) {
				ExportMap exportMap = new ExportMap(rs.getString(1), rs.getString(2));
				exportMapList.add(exportMap);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return exportMapList;
	}

}

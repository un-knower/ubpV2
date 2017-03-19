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
import cn.uway.util.entity.ViewField;

/**
 * 数据源表信息DAO
 * 
 * @author liuchao
 * @ 2013-6-25
 */
public class ViewFieldDAO extends DAO {

	private static final ViewFieldDAO DAO = new ViewFieldDAO();

	private static final Logger logger = LoggerFactory.getLogger(ViewFieldDAO.class);

	// 数据缓存
	private List<ViewField> viewFieldList;

	public static ViewFieldDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取指定数据源的表信息
	 * 
	 * @return 数据源表列表,数据库查不到记录,则返回不可变的空列表
	 * @throws SQLException
	 * </pre>
	 */
	public synchronized List<ViewField> getTableInfos(Connection conn) throws SQLException {
		if (viewFieldList != null)
			return viewFieldList;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select source_name,Aliases_name,view_name from UWAY_VIEW_FIELD";

		try {
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			viewFieldList = new ArrayList<>();

			while (rs.next()) {
				ViewField viewField = new ViewField(rs.getString(1), rs.getString(2), rs.getString(3));
				viewFieldList.add(viewField);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return viewFieldList;
	}

}

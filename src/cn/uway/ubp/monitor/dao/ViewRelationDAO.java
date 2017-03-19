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
import cn.uway.util.entity.ViewRelation;

/**
 * 数据源表信息DAO
 * 
 * @author liuchao
 * @ 2013-6-25
 */
public class ViewRelationDAO extends DAO {

	private static final ViewRelationDAO DAO = new ViewRelationDAO();

	private static final Logger logger = LoggerFactory.getLogger(ViewRelationDAO.class);

	// 数据缓存
	private List<ViewRelation> viewRelationList = null;

	public static ViewRelationDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取指定数据源的表信息
	 * 
	 * @param datasourceId 数据库Id
	 * @return 数据源表列表,数据库查不到记录,则返回不可变的空列表
	 * @throws SQLException
	 * </pre>
	 */
	public synchronized List<ViewRelation> getViewRelations(Connection conn) throws SQLException {
		if (viewRelationList != null)
			return viewRelationList;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "SELECT r.view_name,t.table_name,t.connection_id,t.id FROM UWAY_VIEW_TABLE_RELATION r,UWAY_VIEW_TABLE t WHERE r.table_id=t.id";

		try {
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			viewRelationList = new ArrayList<>();

			while (rs.next()) {
				ViewRelation viewRelation = new ViewRelation(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4));
				viewRelationList.add(viewRelation);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return viewRelationList;
	}

}

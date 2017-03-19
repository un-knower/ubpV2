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
import cn.uway.util.entity.ViewInfo;

/**
 * 数据源表信息DAO
 * 
 * @author liuchao
 * @ 2013-6-25
 */
public class ViewInfoDAO extends DAO {

	private static final ViewInfoDAO DAO = new ViewInfoDAO();

	private static final Logger logger = LoggerFactory.getLogger(ViewInfoDAO.class);

	// 数据缓存
	private List<ViewInfo> viewInfoList;

	public static ViewInfoDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取数据源的表信息
	 * 
	 * @return 数据源表列表,数据库查不到记录,则返回不可变的空列表
	 * @throws SQLException
	 * </pre>
	 */
	public synchronized List<ViewInfo> getTableInfos(Connection conn) throws SQLException {
		if (viewInfoList != null)
			return viewInfoList;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select view_name,view_table_condition,view_table_groupby,view_main_talbe from UWAY_VIEW_INFO";

		try {
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			viewInfoList = new ArrayList<>();

			while (rs.next()) {
				ViewInfo viewInfo = new ViewInfo(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
				viewInfoList.add(viewInfo);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return viewInfoList;
	}

	/**
	 * 获取指定数据源的表信息
	 * @param dataSourceId 数据源Id
	 * @return 数据源表列表,数据库查不到记录,则返回不可变的空列表
	 * @throws SQLException
	 */
	public List<ViewInfo> getViewInfosByDataSourceId(Connection conn, long dataSourceId) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "SELECT lower(view_name) as view_name,view_table_condition,view_table_groupby,lower(view_main_talbe) as view_main_talbe"
				+ " FROM UWAY_VIEW_Info i, ubp_monitor_datasource_table dt"
				+ " WHERE  lower(dt.table_name) = lower(i.view_name) AND dt.datasource_id= ?";

		List<ViewInfo> viewInfoList;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			viewInfoList = new ArrayList<>();

			while (rs.next()) {
				ViewInfo viewInfo = new ViewInfo(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
				viewInfoList.add(viewInfo);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return viewInfoList;
	}

}

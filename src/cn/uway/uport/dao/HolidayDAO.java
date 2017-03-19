package cn.uway.uport.dao;

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
import cn.uway.uport.context.DbPoolManager;

/**
 * 节假日DAO
 * 
 * @author liuchao 2013-11-12
 */
public class HolidayDAO extends DAO {

	private static final HolidayDAO DAO = new HolidayDAO();

	private static final Logger logger = LoggerFactory.getLogger(HolidayDAO.class);

	public static HolidayDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取所有(一定时间范围内)节假日
	 * 
	 * @return 
	 * @throws SQLException
	 * </pre>
	 */
	public List<Timestamp> getHolidays() throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select calendardate from casp_holidays where isholidays = 1 and calendardate > sysdate-730";

		List<Timestamp> holidayList = null;

		try {
			conn = DbPoolManager.getConnectionForHoliday();
			pstmt = conn.prepareStatement(sql);

			log(logger, conn, sql);
			
			rs = pstmt.executeQuery();
			holidayList = new ArrayList<>();

			while (rs.next()) {
				holidayList.add(rs.getTimestamp(1));
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return holidayList;
	}

}

package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;

/**
 * 数据源日志表DAO
 * FIXME 这里需要确认，访问的表是否可以与主库配置相同
 * 
 * @author liuchao
 * @ 2014-03-12
 */
public class DataSourceLogDAO extends DAO {

	private static final DataSourceLogDAO DAO = new DataSourceLogDAO();

	private static final Logger logger = LoggerFactory.getLogger(DataSourceLogDAO.class);

	public static DataSourceLogDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 查询指定数据源和数据时间是否有入库记录，按数据时间升序，取第1条记录
	 * 
	 * 区分正常数据和离散数据：
	 * 正常数据：查数据时间=当前时间
	 * 离散数据：查数据时间>=当前时间
	 * @param tableName
	 * @param dataTime
	 * @return 没有更新数据，返回null
	 * 		有更新数据
	 * 			连续数据
	 * 				查到的数据时间与当前数据时间一致，返回数据时间
	 * 				不一致，返回null
	 * 			离散数据，返回查到的数据时间
	 * @throws SQLException
	 * </pre>
	 */
	public Timestamp checkDataTime(Connection conn, String tableName, Timestamp dataTime) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select t.* from (select data_time,is_discrete from ds_log_data_time_detail where table_name=? and data_time>=? order by data_time asc) t where rownum<2";
		Timestamp _dataTime = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, tableName);
			pstmt.setTimestamp(2, dataTime);

			log(logger, conn, sql, rs, tableName, dataTime);
			
			rs = pstmt.executeQuery();

			if (rs.next()) {
				int isDiscrete = rs.getInt(2);
				if (isDiscrete == 0) {
					// 连续数据
					if (dataTime.equals(rs.getTimestamp(1))) {
						_dataTime = dataTime;
					}
				} else {
					// 离散数据
					_dataTime = rs.getTimestamp(1);
				}
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return _dataTime;
	}

}

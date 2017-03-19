package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;

import cn.uway.framework.connection.pool.db.DBPool;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.dao.ConnectInfoDAO;

/**
 * 
 * 数据过滤，保留数据库查询出的数据
 * 
 * @author zhouq Date 2013-6-18
 */
public class IncludeSqlFilter extends IncludFilter {

	private long connectId;// 数据库连接

	public IncludeSqlFilter(long connectId, String fieldName, String fieldText, String prototype) {
		super(fieldName, "", prototype);
		this.connectId = connectId;
		this.filterText = fieldText;
	}

	@Override
	public void init(Connection taskConn) throws Exception {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			conn = DBPool.createDataSource(ConnectInfoDAO.getInstance().getDatabaseConnectionInfo(taskConn, connectId)).getConnection();
			
			statement = conn.prepareStatement(filterText);
			rs = statement.executeQuery();
			int nColumnCount = rs.getMetaData().getColumnCount();
			fieldValues = new HashSet<String>();
			while (rs.next()) {
				// 注意，有些filter的字段名，和指定的字段名不一样
				if (nColumnCount == 1)
					fieldValues.add(rs.getString(1));
				else
					fieldValues.add(rs.getString(fieldName));
			}

		} catch (Exception e) {
			throw new Exception("初始化<IncludeSqlFilter>失败（" + e.getMessage() +"）", e);
		} finally {
			DatabaseUtil.close(conn, statement, rs);
		}

	}
}

package cn.uway.ubp.monitor.data;

import java.sql.ResultSet;

public class JoinParam {

	public static enum EJoinMode {
		// 外联接
		outJoin,
		// 内联接
		innerJoin,
	};

	// 数据库sql;
	private String sql;

	// 链接ID;
	private long connectId;

	// 链接的字段(如果两个表的字段名不一样，在组装sql时，要as成一样的)
	private String[] joinFields;

	// 链接模式(默认为outJoin
	private int joinModen;

	// 连接结果集
	private ResultSet rs;

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public long getConnectId() {
		return connectId;
	}

	public void setConnectId(long connectId) {
		this.connectId = connectId;
	}

	public String[] getJoinFields() {
		return joinFields;
	}

	public void setJoinFields(String[] joinFields) {
		this.joinFields = joinFields;
	}

	public int getJoinModen() {
		return joinModen;
	}

	public void setJoinModen(int joinModen) {
		this.joinModen = joinModen;
	}

	public ResultSet getRs() {
		return rs;
	}

	public void setRs(ResultSet rs) {
		this.rs = rs;
	}

}

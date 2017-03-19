package cn.uway.util.entity;

/**
 * IncludeSql过滤器实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class IncludeSql extends Filterable {

	/**
	 * 连接Id
	 */
	private int connectionId;

	public IncludeSql(String field, String value, int connectionId) {
		super(field, value);

		this.connectionId = connectionId;
	}

	public int getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(int connectionId) {
		this.connectionId = connectionId;
	}

}

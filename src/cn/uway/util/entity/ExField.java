package cn.uway.util.entity;

public class ExField extends DbSourceField {

	/**
	 * 连接Id
	 */
	private long connectionId;

	public ExField(long tableId, String name, int isIndex, int isExport, long connectionId) {
		super(tableId, name, isIndex, isExport);
		this.connectionId = connectionId;
	}

	public long getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(long connectionId) {
		this.connectionId = connectionId;
	}

}

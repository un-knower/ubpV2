package cn.uway.util.entity;

/**
 * 安全信息实体类定义
 * 
 * @author Chris @ 2013-11-1
 */
public class SecureInfo {

	/**
	 * 调用方ID
	 */
	private int callerId;

	/**
	 * 被调用着程序定义ID
	 */
	private int calledId;

	/**
	 * 调用的用户名
	 */
	private String username;

	/**
	 * 调用的用户密码
	 */
	private String password;

	public SecureInfo(int callerId, int calledId, String username, String password) {
		this.callerId = callerId;
		this.calledId = calledId;
		this.username = username;
		this.password = password;
	}

	public int getCallerId() {
		return callerId;
	}

	public void setCallerId(int callerId) {
		this.callerId = callerId;
	}

	public int getCalledId() {
		return calledId;
	}

	public void setCalledId(int calledId) {
		this.calledId = calledId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}

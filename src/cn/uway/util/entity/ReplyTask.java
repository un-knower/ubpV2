package cn.uway.util.entity;

import java.sql.Timestamp;

public class ReplyTask {

	private String neSysID;

	private Timestamp replyTime;

	public String getNeSysID() {
		return neSysID;
	}

	public ReplyTask(String neSysID, Timestamp replyTime) {
		this.neSysID = neSysID;
		this.replyTime = replyTime;
	}

	public void setNeSysID(String neSysID) {
		this.neSysID = neSysID;
	}

	public Timestamp getReplyTime() {
		return replyTime;
	}

	public void setReplyTime(Timestamp replyTime) {
		this.replyTime = replyTime;
	}

	@Override
	public String toString() {
		return neSysID;
	}

}

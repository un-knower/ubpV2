package cn.uway.util.entity;

/**
 * MyBatis实体类 用于对象化数据记录：程序定义表 构造方法由MyBatis调用，业务代码获取对象属性
 * 
 * @author Chris @ 2013-9-29
 */
public class ProgramDefine {

	private Integer programId;

	private String programName;

	private String programVersion;

	public ProgramDefine(Integer programId, String programName, String programVersion) {
		super();
		this.programId = programId;
		this.programName = programName;
		this.programVersion = programVersion;
	}

	public ProgramDefine() {
		super();
	}

	public Integer getProgramId() {
		return programId;
	}

	public void setProgramId(Integer programId) {
		this.programId = programId;
	}

	public String getProgramName() {
		return programName;
	}

	public void setProgramName(String programName) {
		this.programName = programName;
	}

	public String getProgramVersion() {
		return programVersion;
	}

	public void setProgramVersion(String programVersion) {
		this.programVersion = programVersion;
	}

	@Override
	public String toString() {
		return "ProgramDefine [programId=" + programId + ", programName=" + programName + ", programVersion=" + programVersion + "]";
	}

}

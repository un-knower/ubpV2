package cn.uway.util.entity;

/**
 * 周期实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class PeriodInfo {

	/**
	 * 分析周期
	 */
	private AnalysisPeriod analysisPeriod;

	/**
	 * 监控周期
	 */
	private MonitorPeriod monitorPeriod;

	/**
	 * 频次，可选
	 */
	private OccurTimes occurTimes;

	public PeriodInfo(AnalysisPeriod analysisPeriod, MonitorPeriod monitorPeriod, OccurTimes occurTimes) {
		super();
		this.analysisPeriod = analysisPeriod;
		this.monitorPeriod = monitorPeriod;
		this.occurTimes = occurTimes;
	}

	public AnalysisPeriod getAnalysisPeriod() {
		return analysisPeriod;
	}

	public void setAnalysisPeriod(AnalysisPeriod analysisPeriod) {
		this.analysisPeriod = analysisPeriod;
	}

	public MonitorPeriod getMonitorPeriod() {
		return monitorPeriod;
	}

	public void setMonitorPeriod(MonitorPeriod monitorPeriod) {
		this.monitorPeriod = monitorPeriod;
	}

	public OccurTimes getOccurTimes() {
		return occurTimes;
	}

	public void setOccurTimes(OccurTimes occurTimes) {
		this.occurTimes = occurTimes;
	}

	@Override
	public String toString() {
		return "AddMonitTaskPeriodInfoPart [analysisPeriod=" + analysisPeriod + ", monitorPeriod=" + monitorPeriod + ", occurTimes=" + occurTimes
				+ "]";
	}

}

package cn.uway.util.entity;

import java.util.List;

/**
 * 监控任务过滤器实体类
 * 
 * @author Chris @ 2013-9-29
 */
public class Filter {

	/**
	 * 值包含节点列表
	 */
	private List<Include> includeList;

	/**
	 * 值排除节点列表
	 */
	private List<Exclude> excludeList;

	/**
	 * 值包含节点列表（SQL语句查询）
	 */
	private List<IncludeSql> includeSqlList;

	/**
	 * 值排除节点列表（SQL语句查询）
	 */
	private List<ExcludeSql> excludeSqlList;

	/**
	 * 节假日策略
	 */
	private Holiday holiday;

	public List<Include> getIncludeList() {
		return includeList;
	}

	public void setIncludeList(List<Include> includeList) {
		this.includeList = includeList;
	}

	public List<Exclude> getExcludeList() {
		return excludeList;
	}

	public void setExcludeList(List<Exclude> excludeList) {
		this.excludeList = excludeList;
	}

	public List<IncludeSql> getIncludeSqlList() {
		return includeSqlList;
	}

	public void setIncludeSqlList(List<IncludeSql> includeSqlList) {
		this.includeSqlList = includeSqlList;
	}

	public List<ExcludeSql> getExcludeSqlList() {
		return excludeSqlList;
	}

	public void setExcludeSqlList(List<ExcludeSql> excludeSqlList) {
		this.excludeSqlList = excludeSqlList;
	}

	public Holiday getHoliday() {
		return holiday;
	}

	public void setHoliday(Holiday holiday) {
		this.holiday = holiday;
	}

}

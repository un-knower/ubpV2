package cn.uway.util.entity;

/**
 * 视图表关联关系
 * 
 * @author Chris @ 2013-11-12
 */
public class ExportMap {

	/**
	 * 字段表导出字段
	 */
	private String exportField;

	/**
	 * 告警映射字段
	 */
	private String mapField;

	public ExportMap(String exportField, String mapField) {
		super();
		this.exportField = exportField;
		this.mapField = mapField;
	}

	public String getExportField() {
		return exportField;
	}

	public void setExportField(String exportField) {
		this.exportField = exportField;
	}

	public String getMapField() {
		return mapField;
	}

	public void setMapField(String mapField) {
		this.mapField = mapField;
	}

}

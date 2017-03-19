package cn.uway.ubp.monitor.data.loader.impl.file;

import java.sql.Connection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.data.loader.impl.AbstractLoader;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;

/**
 * <pre>
 * 文件方式的数据加载器的抽象父类
 * 根据类型选择 跨文件数据加载 或 同文件数据加载
 * 目前仅需要支持同文件数据
 * 
 * 20140404
 * 可以考虑备用的CVS读取框架：
 * http://commons.apache.org/proper/commons-csv/	尚未发布正式版本
 * http://sourceforge.net/projects/csvjdbc/			需要进行性能测试
 * 
 * @author Chris 2014-3-11
 * </pre>
 */
public abstract class FileLoader extends AbstractLoader {

	public static FileLoader buildLoader(Connection taskConn, DataSource dataSource) throws Exception {
		List<DbSourceTable> tableList = DataSourceDAO.getInstance().getDbSourceTableList(taskConn, dataSource.getId());
		DbSourceInfo dbSourceInfo = dataSource.getDbSourceInfo();
		dbSourceInfo.setDbSourceTableList(tableList);
		dataSource.setDbSourceInfo(dbSourceInfo);
		
		// 暂不支持加载多文件关联
		boolean isMultipleDatabase = isMultipleFile(tableList);
		if (isMultipleDatabase) {
			throw new UnsupportedOperationException("暂不支持多文件关联加载");
		} else {
			return new SingleFileLoader(dataSource);
		}
	}
	
	public FileLoader(DataSource dataSource) {
		super(dataSource);
	}

	/**
	 * 判断是否跨文件
	 * 
	 * @param tableList
	 * @return
	 */
	private static boolean isMultipleFile(List<DbSourceTable> tableList) {
		return tableList.size() > 1;
	}
	
	/**
	 * 依据字段信息从数据库中获取对应的值组装一条记录GroupingArrayData
	 * 
	 * @param value
	 * @param GroupingArrayData
	 * @param fieldInfo
	 * @throws ParseException
	 */
	protected void buildData(String value, GroupingArrayData GroupingArrayData, FieldIndexInfo fieldInfo) throws ParseException {
		switch (fieldInfo.getFieldType()) {
			case FieldType.BYTE :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setByteValue(fieldInfo.getIndex(), Byte.MAX_VALUE);
				} else {
					GroupingArrayData.setByteValue(fieldInfo.getIndex(), Byte.parseByte(value));
				}
				break;
			case FieldType.INT :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setIntegerValue(fieldInfo.getIndex(), Integer.MAX_VALUE);
				} else {
					GroupingArrayData.setIntegerValue(fieldInfo.getIndex(), Integer.parseInt(value));
				}
				break;
			case FieldType.SHORT :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setShortValue(fieldInfo.getIndex(), Short.MAX_VALUE);
				} else {
					GroupingArrayData.setShortValue(fieldInfo.getIndex(), Short.parseShort(value));
				}
				break;
			case FieldType.LONG :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setLongValue(fieldInfo.getIndex(), Long.MAX_VALUE);
				} else {
					GroupingArrayData.setLongValue(fieldInfo.getIndex(), Long.parseLong(value));
				}
				break;
			case FieldType.FLOAT :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setFloatValue(fieldInfo.getIndex(), Float.MAX_VALUE);
				} else {
					GroupingArrayData.setFloatValue(fieldInfo.getIndex(), Float.parseFloat(value));
				}
				break;
			case FieldType.DOUBLE :
				if (StringUtils.isBlank(value)) {
					GroupingArrayData.setDoubleValue(fieldInfo.getIndex(), Double.MAX_VALUE);
				} else {
					GroupingArrayData.setDoubleValue(fieldInfo.getIndex(), Double.parseDouble(value));
				}
				break;
			case FieldType.DATE :
				GroupingArrayData.setDateValue(fieldInfo.getIndex(), Timestamp.valueOf(value));
				break;
			default : // FieldType.STRING
				GroupingArrayData.setStringValue(fieldInfo.getIndex(), value);
		}
	}

}

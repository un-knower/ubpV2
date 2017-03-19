package cn.uway.ubp.monitor.data.loader.impl.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;

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
 * 数据库方式的数据加载器的抽象父类
 * 根据数据源类型选择 跨库数据源加载 或 同库数据源加载
 * 表形式加载：取数据源日志表有记录则加载
 * 		分为：正常数据加载 和 离散数据加载
 * SQL形式加载：取数据源固定延时加载
 * 
 * @author Chris 2014-3-11
 * </pre>
 */
public abstract class DbLoader extends AbstractLoader {

	public static DbLoader buildLoader(Connection taskConn, DataSource dataSource) throws SQLException {
		List<DbSourceTable> tableList = DataSourceDAO.getInstance().getDbSourceTableList(taskConn, dataSource.getId());
		DbSourceInfo dbSourceInfo = dataSource.getDbSourceInfo();
		dbSourceInfo.setDbSourceTableList(tableList);
		dataSource.setDbSourceInfo(dbSourceInfo);
		
		boolean isMultipleDatabase = isMultipleDatabase(tableList);
		if (isMultipleDatabase) {
			return new MultipleDbLoader(dataSource);
		} else {
			return new SingleDbLoader(dataSource);
		}
	}
	
	public DbLoader(DataSource dataSource) {
		super(dataSource);
	}

	/**
	 * 判断是否跨库
	 * 
	 * @param tableList
	 * @return
	 */
	private static boolean isMultipleDatabase(List<DbSourceTable> tableList) {
		if (tableList.isEmpty() || tableList.size() == 1)
			return false;

		Long connectionId = null;
		for (DbSourceTable table : tableList) {
			if (connectionId == null) {
				connectionId = table.getConnectionId();
				continue;
			}

			if (!connectionId.equals(table.getConnectionId()))
				return true;
		}

		return false;
	}
	
	/**
	 * 依据字段信息从数据库中获取对应的值组装一条记录GroupingArrayData
	 * 
	 * @param rs
	 * @param GroupingArrayData
	 * @param fieldInfo
	 * @throws SQLException
	 * @throws ParseException
	 */
	protected void buildData(ResultSet rs, GroupingArrayData GroupingArrayData, FieldIndexInfo fieldInfo) throws SQLException, ParseException {
		switch (fieldInfo.getFieldType()) {
			case FieldType.BYTE :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setByteValue(fieldInfo.getIndex(), Byte.MAX_VALUE);
				} else {
					GroupingArrayData.setByteValue(fieldInfo.getIndex(), rs.getByte(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.INT :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setIntegerValue(fieldInfo.getIndex(), Integer.MAX_VALUE);
				} else {
					GroupingArrayData.setIntegerValue(fieldInfo.getIndex(), rs.getInt(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.SHORT :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setShortValue(fieldInfo.getIndex(), Short.MAX_VALUE);
				} else {
					GroupingArrayData.setShortValue(fieldInfo.getIndex(), rs.getShort(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.LONG :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setLongValue(fieldInfo.getIndex(), Long.MAX_VALUE);
				} else {
					GroupingArrayData.setLongValue(fieldInfo.getIndex(), rs.getLong(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.FLOAT :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setFloatValue(fieldInfo.getIndex(), Float.MAX_VALUE);
				} else {
					GroupingArrayData.setFloatValue(fieldInfo.getIndex(), rs.getFloat(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.DOUBLE :
				if (rs.getObject(fieldInfo.getFieldName()) == null) {
					GroupingArrayData.setDoubleValue(fieldInfo.getIndex(), Double.MAX_VALUE);
				} else {
					GroupingArrayData.setDoubleValue(fieldInfo.getIndex(), rs.getDouble(fieldInfo.getFieldName()));
				}
				break;
			case FieldType.DATE :
				GroupingArrayData.setDateValue(fieldInfo.getIndex(), rs.getTimestamp(fieldInfo.getFieldName()));
				break;
			default : // FieldType.STRING
				GroupingArrayData.setStringValue(fieldInfo.getIndex(), rs.getString(fieldInfo.getFieldName()));
		}
	}
	
	@Override
	public void finishLoad() {
		// nothing to do in DbLoader
	}

}

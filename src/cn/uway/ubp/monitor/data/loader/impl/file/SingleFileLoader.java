package cn.uway.ubp.monitor.data.loader.impl.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldInfo;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;

/**
 * 单文件方式的数据加载器
 * 
 * @author Chris 2014-4-3
 */
public class SingleFileLoader extends FileLoader {
	
	/**
	 * 数据文件字符集
	 */
	private Charset charset;
	
	/**
	 * 数据文件名日期串格式
	 */
	private static final String DEFAULT_DATE_TIME_FORMAT = "yyyyMMdd-HHmm";

	/**
	 * CSV数据文件扩展名
	 */
	private static final String CSV_EXTENSION = ".csv";
	
	/**
	 * 临时CSV数据文件扩展名
	 */
	private static final String TEMP_CSV_EXTENSION = ".csv.tmp";
	
	/**
	 * 保存加载过的数据文件
	 * 在加载完成后，清除这些文件
	 */
	private List<File> dataFileList = new ArrayList<>();

	private static final Logger logger = LoggerFactory.getLogger(SingleFileLoader.class);

	public SingleFileLoader(DataSource dataSource) {
		super(dataSource);
		charset = Charset.forName(Configuration.getString(Configuration.DATASOURCE_FILE_CHARSET));
	}
	
	/**
	 * <pre>
	 * 读CSV文件数据
	 * 约定
	 * 	1、当出现下一个时间点的数据文件（.tmp或.csv）后，当前时间点数据文件视为完整的，可安全读取
	 * 	2、一个时间点的数据文件可能有多个，[TABLE]为表名，[N]为序号，从1开始
	 * 		命名方式为：[TABLE]-yyyyMMdd-hhmm_[N].csv，示例：MOD_ALARM_PROCESS-20130917-0136_1.csv
	 * 
	 * STEP1：根据当前时间点，查是否存在下一个时间点的数据文件，如果存在，则开始加载当前时间点读取；否则退出（由父类调用getAvailableDataTime完成）
	 * STEP2：加载当前时间点
	 * STEP3：循环所有数据文件，逐一加载；表头信息只读取一次
	 * </pre>
	 */
	@Override
	public BlockData load() throws Exception {
		try {
			DbSourceInfo dbSource = dataSource.getDbSourceInfo();
			List<DbSourceTable> tableList = dbSource.getDbSourceTableList();
			DbSourceTable table = tableList.get(0);
			String tableName = table.getName();
			
			BlockData blockData = new BlockData();
			int counter = 1; // 文件计数器，从1开始
			
			long startTime = System.currentTimeMillis();
			
			do {
				File file = getDataFile(Configuration.getString(Configuration.DATASOURCE_FILE_ROOT_DIR), tableName, dataSource.getDataTime(), counter);
				if (!file.exists() || !file.isFile())
					break;
				
				logger.debug("数据源[{}]，数据时间[{}]，读取数据文件{}", new Object[]{dataSource.getId(), dataSource.getDataTime(), counter});
				
				readCSV(blockData, file, counter);
				
				dataFileList.add(file);
				
				logger.debug("数据源[{}]，数据时间[{}]，读取数据文件{}完成", new Object[]{dataSource.getId(), dataSource.getDataTime(), counter});
				
				counter++;
			} while (true);
			
			long endTime = System.currentTimeMillis();
			logger.debug("数据源[{}]，数据时间[{}]，完成加载，耗时{}ms", new Object[]{dataSource.getId(), dataSource.getDataTime(), (endTime-startTime)});
			
			return blockData;
		} catch (IOException e) {
			logger.error("数据源[{}]，数据时间[{}]，加载数据异常", new Object[]{dataSource.getId(), dataSource.getDataTime(), e});
			return null;
		}
	}
	
	/**
	 * 读取一个CSV数据文件
	 * @param blockData
	 * @param file
	 * @param idx
	 * @throws Exception 
	 */
	private void readCSV(BlockData blockData, File file, int idx) throws Exception {
		int count = 0; // 加载的数据条数

		CSVReader reader = null;
		
		try {
			logger.debug("数据源[{}]，数据时间[{}]，开始读取{}，文件长度：{}mb", new Object[]{dataSource.getId(), dataSource.getDataTime(), idx, file.length()/1024/1024});
			long loadStartTime = System.currentTimeMillis();
			
			reader = new CSVReader(new BufferedReader(new InputStreamReader(new FileInputStream(file), charset)));
			// 当读取的文件较大，占用内存较多时，可考虑逐行读取；权衡性能与资源占用时，可依据文件大小选择不同读取方式
			List<String[]> rowList = reader.readAll();
			
			long loadEndTime = System.currentTimeMillis();
			logger.debug("数据源[{}]，数据时间[{}]，完成读取，共计{}行数据，耗时{}ms", new Object[]{dataSource.getId(), dataSource.getDataTime(), rowList.size(), (loadEndTime-loadStartTime)});
			
			logger.debug("数据源[{}]，数据时间[{}]，开始数据组装", new Object[]{dataSource.getId(), dataSource.getDataTime()});
			long assembleStartTime = System.currentTimeMillis();
			
			// 获取元数据（字段和数据类型）
			String[] fieldNameAry = rowList.remove(0);
			String[] fieldTypeAry = rowList.remove(0);
			
			// 去掉第一列前的"#"
			fieldNameAry[0] = fieldNameAry[0].substring(1);
			fieldTypeAry[0] = fieldTypeAry[0].substring(1);
			
			Connection taskConn = null;
			List<FieldInfo> fieldList = new ArrayList<FieldInfo>();
			try{
				taskConn = DbPoolManager.getConnectionForTask();
				
				fieldList = DataSourceDAO.getInstance().getDbSourceFieldList(taskConn, dataSource.getId());
			}finally{
				DatabaseUtil.close(taskConn);
			}
			
			List<FieldInfo> indexKeyList = new ArrayList<>();
			List<FieldInfo> exportFieldsList = new ArrayList<>();
			for (FieldInfo field : fieldList) {
				if (field.isIndexFlag()) {
					indexKeyList.add(field);
				}
				
				if (field.isExportFlag()) {
					exportFieldsList.add(field);
				}
			}

			Map<String, String> metaMap = getMetaMap(fieldNameAry, fieldTypeAry);
			fieldList = getFieldInfosHaveType(fieldList, metaMap);
			
			// 表头元数据只设置一次
			if (idx == 1) {
				GroupingArrayDataDescriptor descriptor = new GroupingArrayDataDescriptor(fieldList);
				blockData.setMetaInfo(descriptor);
				
				blockData.setIndexKey(StringUtils.join(indexKeyList.iterator(), MonitorConstant.KEY_SPLIT));
				if (!exportFieldsList.isEmpty()) {
					blockData.setExportFieldsKey(StringUtils.join(exportFieldsList.iterator(), MonitorConstant.KEY_SPLIT));
				}
				
				Map<String, GroupingArrayData> groupingArrayDatas = new HashMap<String, GroupingArrayData>();
				blockData.setGroupingArrayDatas(groupingArrayDatas);
			}
			
			// 确定字段索引顺序
			// <索引, 列名>
			Map<String, Integer> columnIdxMap = new HashMap<>();
			for (int i=0; i<fieldNameAry.length; i++) {
				String field = fieldNameAry[i];
				columnIdxMap.put(field, i);
			}
			
			for (String[] row : rowList) {
				// 循环每一列，添加数据
				GroupingArrayDataDescriptor descriptor = blockData.getMetaInfo();
				GroupingArrayData rowData = new GroupingArrayData(descriptor);
				for (FieldInfo field : fieldList) {
					FieldIndexInfo fieldIndexInfo = descriptor.getFileIndexInfo(field.getName().toLowerCase());
					if (fieldIndexInfo == null)
						continue;
					
					String fieldName = fieldIndexInfo.getFieldName();
					int index = columnIdxMap.get(fieldName);
					String data = row[index];
					
					buildData(data, rowData, fieldIndexInfo);
				}
				
				// 循环添加索引
				List<String> indexValueList = new ArrayList<String>();
				for (FieldInfo fieldInfoIndex : indexKeyList) {
					int index = columnIdxMap.get(fieldInfoIndex.getName());
					String data = row[index];
					
					indexValueList.add(getIndexValue(data, fieldInfoIndex));
				}
				
				// 处理完一行数据，加到数据集中，以索引为主键
				Map<String, GroupingArrayData> groupingArrayDatas = blockData.getGroupingArrayDatas();
				groupingArrayDatas.put(StringUtils.join(indexValueList.iterator(), MonitorConstant.KEY_SPLIT), rowData);
				count++;
			}
			
			long assembleEndTime = System.currentTimeMillis();
			logger.debug("数据源[{}]，数据时间[{}]，完成数据组装，共计{}条记录，耗时{}ms", new Object[]{dataSource.getId(), dataSource.getDataTime(), count, (assembleEndTime-assembleStartTime)});
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	/**
	 * 获取元数据内容
	 * @param fieldNameAry 字段名
	 * @param fieldTypeAry 字段数据类型
	 * @return Map<字段, 数据类型>
	 * @throws IOException
	 */
	private Map<String, String> getMetaMap(String[] fieldNameAry, String[] fieldTypeAry) throws IOException {
		if (fieldNameAry.length != fieldTypeAry.length)
			throw new IllegalArgumentException("字段数量与数据类型定义数量不一致");

		Map<String, String> metaMap = new HashMap<>();		
		for (int i=0; i<fieldNameAry.length; i++) {
			metaMap.put(fieldNameAry[i], fieldTypeAry[i]);
		}
		
		return metaMap;
	}

	/**
	 * 获取文件名
	 * @param dataSourceFileRootDir
	 * @param name
	 * @param dataTime
	 * @praam idx
	 * @return
	 * @throws IOException
	 */
	private File getDataFile(String dataSourceFileRootDir, String name, Date dataTime, int idx) throws IOException {
		File dir = new File(dataSourceFileRootDir, name);
		String fileName = name + "-" + new SimpleDateFormat(DEFAULT_DATE_TIME_FORMAT).format(dataTime) + "_" + idx + CSV_EXTENSION ;
		
		return new File(dir, fileName);
	}

	/**
	 * <pre>
	 * 根据当前时间点，查是否存在下一个时间点的数据文件（数据文件.csv 或 临时数据文件.csv.tmp）
	 * 如果存在，返回当前时间点；否则返回null
	 * </pre>
	 */
	@Override
	public Timestamp getAvailableDataTime(Timestamp currDataTime) throws Exception {
		DbSourceInfo dbSource = dataSource.getDbSourceInfo();
		List<DbSourceTable> tableList = dbSource.getDbSourceTableList();
		DbSourceTable table = tableList.get(0);
		String tableName = table.getName();
		Date nextDataTime = DateGranularityUtil.forwardTimeTravel(currDataTime, dataSource.getGranularity().toString(), 1);
		
		File dir = new File(Configuration.getString(Configuration.DATASOURCE_FILE_ROOT_DIR), tableName);
		String _fileName = tableName + "-" + new SimpleDateFormat(DEFAULT_DATE_TIME_FORMAT).format(nextDataTime) + "_1";
		
		File file;
		
		file = new File(dir, _fileName + CSV_EXTENSION);
		if (file.exists() && file.isFile())
			return currDataTime;
		
		file = new File(dir, _fileName + TEMP_CSV_EXTENSION);
		if (file.exists() && file.isFile())
			return currDataTime;
		
		return null;
	}

	/**
	 * 完成加载：删除当前时间点的数据文件
	 */
	@Override
	public void finishLoad() {
		for (File file : dataFileList) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				logger.error("数据文件删除失败：{}", file.getPath(), e);
			}
		}
		
		dataFileList.clear();
	}

	/**
	 * 根据数据的类型，从数据集合中获取数据
	 * 
	 * @param Object obj
	 * @param fieldInfo
	 * @return
	 */
	private String getIndexValue(Object obj, FieldInfo fieldInfo) {
		if (obj == null) {
			return "#";
		}
		
		switch (fieldInfo.getType()) {
			case FieldType.BYTE :
				return String.valueOf(Byte.parseByte(obj.toString()));
			case FieldType.INT :
				return String.valueOf(Integer.parseInt(obj.toString()));
			case FieldType.SHORT :
				return String.valueOf(Short.parseShort(obj.toString()));
			case FieldType.LONG :
				return String.valueOf(Long.parseLong(obj.toString()));
			case FieldType.FLOAT :
				return String.valueOf(Float.parseFloat(obj.toString()));
			case FieldType.DOUBLE :
				return String.valueOf(Double.parseDouble(obj.toString()));
			case FieldType.STRING :
				return obj.toString();
			case FieldType.DATE :
				return obj.toString();
			default :
				throw new NullPointerException("键值" + fieldInfo.getName() + "未知类型");
		}

	}

	/**
	 * 通过数据连接集来获取对应字段的数据类型
	 * 
	 * @param fieldList
	 * @param metaMap
	 * @return
	 * @throws SQLException
	 */
	private List<FieldInfo> getFieldInfosHaveType(List<FieldInfo> fieldList, Map<String, String> metaMap){
		for (FieldInfo field : fieldList) {
			String fieldName = field.getName();
			String fieldType = metaMap.get(fieldName);
			field.setType(fieldType);
		}

		return fieldList;
	}

}

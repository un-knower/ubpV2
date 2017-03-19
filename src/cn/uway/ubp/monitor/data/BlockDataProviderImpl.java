package cn.uway.ubp.monitor.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldInfo;
import cn.uway.framework.data.model.FieldInfoExtend;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.OperatorFileSerial;
import cn.uway.framework.util.OperatorFileSerial.EOPERATOR_FILE_MODE;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.util.entity.DataSource;

/**
 * 从本地文件反序列化成数据实现
 * 
 * @author Administrator @ 2013-7-23
 */
public class BlockDataProviderImpl implements BlockDataProvider {

	protected static final Logger logger = LoggerFactory
			.getLogger(BlockDataProviderImpl.class);

	// 文件头缓存
	Map<String, Header> headerMap = new ConcurrentHashMap<String, Header>();

	/**
	 * 数据列信息类
	 * 
	 * @author ShiGang @ 2013-7-22
	 */
	public static class ColumnInfo {

		// 城市ID
		short cityID;

		// 记录数
		int recordCount;

		// 偏移位置
		long offset;

		// 长度
		int length;

		// 列的类型
		short columnType;

		// 列的信息
		FieldIndexInfo fieldInfo;
	}

	public static final String DS_SAVE_PATH = "cachefile";

	private BlockDataProviderImpl() {
		super();
	}

	private static class MyObjectContainer {

		private static BlockDataProvider instance = new BlockDataProviderImpl();
	}

	public static BlockDataProvider getInstance() {
		return MyObjectContainer.instance;
	}

	@Override
	public BlockData load(DataSource dsInfo, int cityID, List<String> columns,
			Date date, Busy busy) throws Exception {
		// 索引文件路径
		String idxPath = getPath(dsInfo) + DateTimeUtil.formatDateTimeStr(date);

		return buildData(idxPath, cityID, columns, busy);
	}

	/**
	 * <pre>
	 * 根据数据源获取对应序列化文件目录 从内存中找文件路径
	 * 1.没有从数据库中查询信息组装成路径信息，并缓存到内存中
	 * 2.找到返回文件路径
	 * 
	 * @param dataSourceId
	 * @return
	 * </pre>
	 */
	@Override
	public String getPath(DataSource dataSource) {
		StringBuilder serializeKey = new StringBuilder();
		serializeKey.append(Configuration.ROOT_DIRECTORY)
				.append(File.separator).append(DS_SAVE_PATH)
				.append(File.separator).append(dataSource.getNetType())
				.append(File.separator).append(dataSource.getNeLevel())
				.append(File.separator).append(dataSource.getGranularity())
				.append(File.separator).append(dataSource.getId())
				.append(File.separator);
		return serializeKey.toString();
	}

	private boolean deleteFileOnExist(String fileName) {
		try {
			File file = new File(fileName);
			if (file.exists() && !file.isDirectory())
				return file.delete();
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public int save(DataSource dataSource, BlockData blockData)
			throws Exception {
		if (blockData == null)
			throw new Exception(
					"BlockDataProviderImpl::save() datasourceInfo为null.");

		if (dataSource == null)
			throw new Exception("BlockDataProviderImpl::save() blockData为null.");

		// 先取出city_id对应的字段信息;
		GroupingArrayDataDescriptor metaInfo = blockData.getMetaInfo();
		FieldIndexInfo fdCityInfo = metaInfo.getFileIndexInfo("city_id");
		if (fdCityInfo == null) {
			throw new Exception("在BlockData中，不存在CITY_ID的列");
		}

		// 取出列的数量
		int nFiledCount = metaInfo.getFileIndexInfos().size();
		if (nFiledCount < 1)
			throw new Exception("BlockData无其它可系列化字段");

		// 取出其它的field信息到一个数组中;
		FieldIndexInfo[] fieldColumnInfos = new FieldIndexInfo[nFiledCount];
		Iterator<Entry<String, FieldIndexInfo>> iterFieldInfo = metaInfo
				.getFileIndexInfos().entrySet().iterator();
		int i = 0;
		while (iterFieldInfo.hasNext()) {
			Entry<String, FieldIndexInfo> entry = iterFieldInfo.next();
			fieldColumnInfos[i++] = entry.getValue();
		}

		// 数据集合对象<CITY_ID, LIST<GroupingArrayData>记录集>
		Map<Short, List<GroupingArrayData>> mapCityRecords = new HashMap<Short, List<GroupingArrayData>>();

		// 记录总数
		int totalRecordCount = 0;

		// 先将blockData的数据，按CITY_ID，存放到对应的list中；
		Iterator<Entry<String, GroupingArrayData>> iter = blockData
				.getGroupingArrayDatas().entrySet().iterator();
		while (iter.hasNext()) {
			GroupingArrayData record = iter.next().getValue();
			if (record == null)
				continue;

			// GroupingArrayData存储的整形都是基础类型，所以这里不判断null;
			Short cityID = ((Number) record.getPropertyValue(fdCityInfo))
					.shortValue();
			List<GroupingArrayData> lstCityRecords = null;
			if (mapCityRecords.containsKey(cityID)) {
				lstCityRecords = mapCityRecords.get(cityID);
			} else {
				lstCityRecords = new LinkedList<GroupingArrayData>();
				mapCityRecords.put(cityID, lstCityRecords);
			}

			lstCityRecords.add(record);
			++totalRecordCount;
		}

		// 文件存储路径 ./nettype/level/gran/date.raw.gz
		// StringBuilder serializableKey = new StringBuilder();
		// serializableKey.append(dataSource.getNetType()).append(File.separator)
		// .append(dataSource.getNeLevel()).append(File.separator)
		// .append(dataSource.getGranularity()).append(File.separator)
		// .append(dataSource.getId());

		// 文件
		// String fileDir = System.getProperties().get("user.dir").toString() +
		// File.separator + DS_SAVE_PATH + File.separator +
		// serializableKey.toString() + File.separator;
		// String fileName = fileDir +
		// DateTimeUtil.formatDateTimeStr(dataSource.getDataTime());
		String fileDir = getPath(dataSource);
		String fileName = fileDir
				+ DateTimeUtil.formatDateTimeStr(dataSource.getDataTime());

		// 创建目录
		File file = new File(fileDir);
		if (!file.exists() || !file.isDirectory()) {
			file.mkdirs();
		}

		// 创建索引文件(如有异常，会自动抛出)
		String indexFileName = fileName + ".idx";
		String indexTmpfileName = fileName + ".idx.tmp";

		if (!deleteFileOnExist(indexTmpfileName)) {
			logger.warn("数据源[{}]，删除文件[{}]失败，文件可能正在被其它任务读取中，数据将在下一个周期尝试加载",
					dataSource.getId(), indexTmpfileName);
			return -1;
		}

		if (!deleteFileOnExist(indexFileName)) {
			logger.warn("数据源[{}]，删除文件[{}]失败，文件可能正在被其它任务读取中，数据将在下一个周期尝试加载",
					dataSource.getId(), indexFileName);
			return -1;
		}

		// 创建数据文件(如有异常，会自动抛出)
		String dataFileName = fileName + ".bin";
		if (!deleteFileOnExist(dataFileName)) {
			logger.warn("数据源[{}]，删除文件[{}]失败，文件可能正在被其它任务读取中，数据将在下一个周期尝试加载",
					dataSource.getId(), dataFileName);
			return -1;
		}

		OperatorFileSerial ofsIdx = null;
		OperatorFileSerial ofsData = null;

		try {
			ofsIdx = new OperatorFileSerial(
					OperatorFileSerial.EOPERATOR_FILE_MODE.e_Write,
					indexTmpfileName);
			ofsData = new OperatorFileSerial(
					OperatorFileSerial.EOPERATOR_FILE_MODE.e_Write, dataFileName);
			
			int cityCount = mapCityRecords.size();
			int columnCount = fieldColumnInfos.length;
			// 列信息 [城市数量][列的数量]，将每列的记录数，偏移位置都统计在这个二维数组中
			ColumnInfo[][] columnCityInfos = new ColumnInfo[cityCount][columnCount];
			// 写数据文件到*.bin
			{
				// 遍历每个城市的数据块
				int cityIndex = 0;
				Iterator<Entry<Short, List<GroupingArrayData>>> iterCityRecords = mapCityRecords
						.entrySet().iterator();
				while (iterCityRecords.hasNext()) {
					Entry<Short, List<GroupingArrayData>> entry = iterCityRecords
							.next();
					Short cityID = entry.getKey();
					List<GroupingArrayData> lstRecords = entry.getValue();
					// 写入每个城市，每一列的数据
					for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
						columnCityInfos[cityIndex][columnIndex] = new ColumnInfo();
						columnCityInfos[cityIndex][columnIndex].cityID = cityID;
						columnCityInfos[cityIndex][columnIndex].fieldInfo = fieldColumnInfos[columnIndex];
						columnCityInfos[cityIndex][columnIndex].columnType = FieldType
								.getType(fieldColumnInfos[columnIndex]
										.getFieldType());
	
						// 写入当前城市cityIndex的当前列columnIndex数据，并统计偏移位置，数据长度
						saveColumn(ofsData,
								columnCityInfos[cityIndex][columnIndex], lstRecords);
					}
	
					++cityIndex;
				}
			}
	
			// 写头文件到*.idx
			{
				// 写文件头
				{
					// 总记录数
					ofsIdx.write((int) totalRecordCount);
					// BlockData的附加信息 indexKey
					ofsIdx.write((String) blockData.getIndexKey());
					// BlockData的附加信息 exportFieldsKey
					ofsIdx.write((String) blockData.getExportFieldsKey());
				}
	
				// 写列的描述信息
				{
					// 列的数量
					ofsIdx.write((short) columnCount);
					for (i = 0; i < columnCount; ++i) {
						// 列名
						ofsIdx.write((String) fieldColumnInfos[i].getFieldName());
						// 列类型
						ofsIdx.write((byte) FieldType.getType(fieldColumnInfos[i]
								.getFieldType()));
					}
				}
	
				// 写列存储的位置信息
				{
					// 城市数量
					ofsIdx.write((short) columnCityInfos.length);
					for (int cityIndex = 0; cityIndex < columnCityInfos.length; ++cityIndex) {
						// 城市ID
						ofsIdx.write((short) columnCityInfos[cityIndex][0].cityID);
						// 当前城市的记录数
						ofsIdx.write((int) columnCityInfos[cityIndex][0].recordCount);
	
						// 写入当前城市每一列的存储信息
						for (int columnIndex = 0; columnIndex < columnCityInfos[cityIndex].length; ++columnIndex) {
							ColumnInfo columnInfo = columnCityInfos[cityIndex][columnIndex];
							// 偏移位置
							ofsIdx.write((long) columnInfo.offset);
							// 数据块尺寸
							ofsIdx.write((int) columnInfo.length);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("数据源[{}]序列化失败，数据将在下一个周期尝试加载",
							new Object[]{dataSource.getId(), e});
			return  -1;
		} finally {
			try {
				ofsData.close();
			} catch (Exception e) {
				logger.error("文件操作句柄关闭失败", e);
			}
			
			try {
				ofsIdx.close();
			} catch (Exception e) {
				logger.error("文件操作句柄关闭失败", e);
			}
		}

		// 将文件名从tmp改名
		File fileTmp = new File(indexTmpfileName);
		if (!fileTmp.renameTo(new File(indexFileName))) {
			logger.warn(
					"数据源[{}]，文件改名失败[{}] -> [{}]，文件可能正在被其它任务读取中，数据将在下一个周期尝试加载",
					new Object[]{dataSource.getId(), indexTmpfileName,
							indexFileName});
			return -1;
		}
		
		// 文件发生变化 清除缓存
		headerMap.remove(fileName);

		return 0;
	}

	// 保存列
	public void saveColumn(OperatorFileSerial ofsData, ColumnInfo columnInfo,
			List<GroupingArrayData> lstData) throws Exception {
		columnInfo.offset = ofsData.getCurrFileWriteLength();
		Iterator<GroupingArrayData> iter = lstData.iterator();
		int nCurrCityRecordCount = 0;
		while (iter.hasNext()) {
			GroupingArrayData record = iter.next();

			// Object value = record.getPropertyValue(columnInfo.fieldInfo);
			switch (columnInfo.columnType) {
				case 1 :	// FieldType.BYTE
					ofsData.write((Byte) record.byteValue(columnInfo.fieldInfo
							.getIndex()));
					break;
				case 2 :	// FieldType.SHORT
					ofsData.write((Short) record
							.shortValue(columnInfo.fieldInfo.getIndex()));
					break;
				case 3 :	// FieldType.INT
					ofsData.write((Integer) record
							.integerValue(columnInfo.fieldInfo.getIndex()));
					break;
				case 4 :	// FieldType.LONG
					ofsData.write((Long) record.longValue(columnInfo.fieldInfo
							.getIndex()));
					break;
				case 5 :	// FieldType.FLOAT
					ofsData.write((Float) record
							.floatValue(columnInfo.fieldInfo.getIndex()));
					break;
				case 6 :	// FieldType.DOUBLE
					ofsData.write((Double) record
							.doubleValue(columnInfo.fieldInfo.getIndex()));
					break;
				case 7 :	// FieldType.STRING
					ofsData.write((String) record
							.stringValue(columnInfo.fieldInfo.getIndex()));
					break;
				case 8 :	// FieldType.DATE
					ofsData.write((Date) record.dateValue(columnInfo.fieldInfo
							.getIndex()));
					break;
				default :
					throw new Exception("未知的数据类型");
			}

			++nCurrCityRecordCount;
		}

		ofsData.flush();
		columnInfo.length = (int) (ofsData.getCurrFileWriteLength() - columnInfo.offset);
		columnInfo.recordCount = nCurrCityRecordCount;
	}

	/**
	 * 数据头信息
	 * 
	 * @author zqing @ 2013-7-24
	 */
	private class Header {

		// 数据记录数
		@SuppressWarnings("unused")
		int size;

		// 记录索引
		String indexKey;

		// 导出字段列表
		String exportFieldsKey;

		// filedName FieldInfo
		Map<String, FieldInfo> fieldInfosMap;

		// city fieldName offset
		Map<Short, Map<String, OffSet>> cityMap;
	}

	/**
	 * 数据列的对应在整列数据文件中的起始位置 和数据长度
	 * 
	 * @author zqing @ 2013-7-24
	 */
	private class OffSet {

		long off;

		int length;

		int count;
	}

	/**
	 * 组装头
	 * 
	 * @return
	 * @throws Exception
	 */
	private Header buildHeader(String path) throws Exception {
		OperatorFileSerial rafIdx = null;
		try {
			Header header = new Header();
			// 索引文件
			try {
				rafIdx = new OperatorFileSerial(EOPERATOR_FILE_MODE.e_Read,
						path + ".idx");
			} catch (Exception e) {
				Log.warn(e.getMessage());
				return null;
			}
			// 记录数
			int size = rafIdx.read_int();
			header.size = size;
			// indexKey属性
			String indexKey = rafIdx.read_string();
			header.indexKey = indexKey;
			header.exportFieldsKey = rafIdx.read_string();
			// 列的数量
			short columnCount = rafIdx.read_short();

			Map<String, FieldInfo> fieldInfosMap = new LinkedHashMap<String, FieldInfo>();
			for (int j = 0; j < columnCount; j++) {
				String fieldName = rafIdx.read_string().toLowerCase();
				FieldInfoExtend fieldInfo = new FieldInfoExtend();
				// 字段名
				fieldInfo.setName(fieldName);
				// 字段类型
				fieldInfo.setType(FieldType.getType(rafIdx.read_byte()));
				// 存储文件索引位置
				fieldInfo.setColumnindex(j);
				fieldInfosMap.put(fieldName, fieldInfo);
			}
			header.fieldInfosMap = fieldInfosMap;
			// 城市个数
			short cityCount = rafIdx.read_short();
			for (int k = 0; k < cityCount; k++) {
				// 城市Id
				short city_id = rafIdx.read_short();
				if (header.cityMap == null) {
					Map<Short, Map<String, OffSet>> cityMap = new HashMap<Short, Map<String, OffSet>>();
					header.cityMap = cityMap;
				}
				if (header.cityMap.get(city_id) == null) {
					Map<String, OffSet> fieldMap = new HashMap<String, OffSet>();
					header.cityMap.put(city_id, fieldMap);
				}

				// 城市记录数
				int cityNum = rafIdx.read_int();
				for (Entry<String, FieldInfo> entry : fieldInfosMap.entrySet()) {
					// bin数据开始位置
					long off = rafIdx.read_long();
					// bin数据长度
					int length = rafIdx.read_int();
					OffSet offSet = new OffSet();
					offSet.count = cityNum;
					offSet.off = off;
					offSet.length = length;
					// fieldName获取作为键值加入
					header.cityMap.get(city_id).put(entry.getKey(), offSet);
				}
			}
			return header;
		} finally {
			if (rafIdx != null)
				rafIdx.close();
		}
	}

	/**
	 * 组装BlockData数据
	 * 
	 * @param cityID
	 * @param columns
	 * @return
	 * @throws Exception
	 */
	public BlockData buildData(String path, int cityID, List<String> columns,
			Busy busy) throws Exception {
		OperatorFileSerial rafbin = null;
		try {
			Header header = headerMap.get(path);
			// 文件头缓存
			if (header == null) {
				header = buildHeader(path);
				if (header == null)
					return null;

				headerMap.put(path, header);
			}

			BlockData blockData = new BlockData();
			blockData.setIndexKey(header.indexKey);
			blockData.setExportFieldsKey(header.exportFieldsKey);
			// 字段所有列数
			Map<String, FieldInfo> fieldInfosMap = header.fieldInfosMap;
			// 需要处理字段列数
			List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();
			for (String fieldName : columns) {
				if (fieldInfosMap.get(fieldName.toLowerCase()) == null) {
					throw new IllegalArgumentException("该字段"
							+ fieldName.toLowerCase() + "不存在");
				}
				fieldInfos.add(fieldInfosMap.get(fieldName.toLowerCase()));
			}
			GroupingArrayDataDescriptor groupingArrayDataDescriptor = new GroupingArrayDataDescriptor(
					fieldInfos);
			blockData.setMetaInfo(groupingArrayDataDescriptor);
			List<GroupingArrayData> groupingArrayDataList = new ArrayList<GroupingArrayData>();
			// 城市个数
			// 数据文件
			rafbin = new OperatorFileSerial(EOPERATOR_FILE_MODE.e_Read, path
					+ ".bin");
			Map<Short, Map<String, OffSet>> cityMap = header.cityMap;
			// cityID为0读取所有的数据，即为全省
			if (cityID == 0) {
				for (Entry<Short, Map<String, OffSet>> cityEntry : cityMap
						.entrySet()) {
					// 城市Id
					Map<String, OffSet> fieldMap = cityEntry.getValue();
					groupingArrayDataList.addAll(buildGroupIngArrayDateList(
							fieldMap, fieldInfos, groupingArrayDataDescriptor,
							rafbin));
				}
			} else {
				Map<String, OffSet> fieldMap = cityMap.get((short) cityID);
				if (fieldMap == null)
					throw new IllegalArgumentException("城市ID：" + cityID + "不存在");
				groupingArrayDataList.addAll(buildGroupIngArrayDateList(
						fieldMap, fieldInfos, groupingArrayDataDescriptor,
						rafbin));
			}
			// 索引字段
			String index = blockData.getIndexKey();
			Map<String, GroupingArrayData> groupingArrayDataByMap = rebuildGroupingArrayDataByMap(
					groupingArrayDataList, groupingArrayDataDescriptor, index,
					busy);
			blockData.setGroupingArrayDatas(groupingArrayDataByMap);
			return blockData;
		} catch (Exception e) {
			logger.error("文件读取错误,key：{}，cityId:{}", path, cityID);
			// 出错后，将该文件的缓存移除
			headerMap.remove(path);
			throw e;
		} finally {
			if (rafbin != null)
				rafbin.close();
		}
	}

	/**
	 * 读取数据文件组装成城市的所有记录的 GroupingArrayData
	 * 
	 * @param fieldMap
	 * @param fieldInfos
	 * @param groupingArrayDataDescriptor
	 * @param groupingArrayDataList
	 * @param rafbin
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private List<GroupingArrayData> buildGroupIngArrayDateList(
			Map<String, OffSet> fieldMap, List<FieldInfo> fieldInfos,
			GroupingArrayDataDescriptor groupingArrayDataDescriptor,
			OperatorFileSerial rafbin) throws IOException {
		List<GroupingArrayData> groupingArrayDataList = new ArrayList<GroupingArrayData>();
		// 城市记录数存储在offSet中，记录对象只初始化一次，使用标志
		boolean flag = true;
		for (FieldInfo fieldInfo : fieldInfos) {
			FieldInfoExtend fieldInfoExtend = (FieldInfoExtend) fieldInfo;
			OffSet offSet = fieldMap.get(fieldInfo.getName());
			// 城市记录数
			int cityNum = offSet.count;
			if (flag) {
				for (int n = 0; n < cityNum; n++) {
					// 对应一条记录对象
					GroupingArrayData groupingArrayData = new GroupingArrayData(
							groupingArrayDataDescriptor);
					groupingArrayDataList.add(groupingArrayData);
				}
				flag = false;
			}

			// bin数据开始位置
			long offset = offSet.off;
			// bin数据长度
			int length = offSet.length;
			rafbin.seek(offset);
			// 获取对应子字段字节数
			int byteSize = FieldType.getType(fieldInfoExtend.getType());
			FieldIndexInfo fieldIndexInfo = groupingArrayDataDescriptor
					.getFileIndexInfo(fieldInfoExtend.getName().toLowerCase());
			setGroupingArrayData(groupingArrayDataList, fieldIndexInfo, rafbin,
					fieldInfoExtend.getType());
		}

		return groupingArrayDataList;
	}

	/**
	 * 一条数据选取键值为Key
	 * 
	 * @param groupingArrayDataList
	 * @param groupingArrayDataDescripto
	 * @return
	 * @throws DocumentException
	 */
	private Map<String, GroupingArrayData> rebuildGroupingArrayDataByMap(
			List<GroupingArrayData> groupingArrayDataList,
			GroupingArrayDataDescriptor groupingArrayDataDescripto,
			String index, Busy busy) throws DocumentException {
		String[] indexArr = index.split(MonitorConstant.KEY_SPLIT);
		Map<String, GroupingArrayData> groupingArrayDatas = new HashMap<String, GroupingArrayData>();
		for (GroupingArrayData groupingArrayData : groupingArrayDataList) {
			// 忙时数据过滤
			if (busy != null
					&& busy.onBusy(groupingArrayData,
							groupingArrayDataDescripto, index))
				continue;
			List<Object> indexList = new ArrayList<Object>();
			for (String idx : indexArr) {
				Object obj = groupingArrayData
						.getPropertyValue(groupingArrayDataDescripto
								.getFileIndexInfo(idx.toLowerCase()));
				if (obj == null) {
					indexList.add("NuL");
				} else if (obj instanceof String) {
					indexList.add(obj);
				} else if (obj instanceof Number) {
					Long number = ((Number) obj).longValue();
					indexList.add(number.toString());
				} else {
					logger.warn("不支持的数据类型{}作为key值", obj.getClass().getName());
				}
			}
			groupingArrayDatas.put(
					StringUtils.join(indexList, MonitorConstant.KEY_SPLIT),
					groupingArrayData);
		}

		return groupingArrayDatas;
	}

	/**
	 * 获取数据文件中一列所有数据逐个放入GroupingArrayData
	 * 
	 * @param groupingArrayDataList
	 * @param fieldIndexInfo
	 * @param raf
	 * @param type
	 * @throws IOException
	 */
	public void setGroupingArrayData(
			List<GroupingArrayData> groupingArrayDataList,
			FieldIndexInfo fieldIndexInfo, OperatorFileSerial raf, String type)
			throws IOException {
		for (GroupingArrayData groupingArrayData : groupingArrayDataList) {
			switch (type) {
				case FieldType.BYTE :
					groupingArrayData.setByteValue(fieldIndexInfo.getIndex(),
							raf.read_byte());
					break;
				case FieldType.SHORT :
					groupingArrayData.setShortValue(fieldIndexInfo.getIndex(),
							raf.read_short());
					break;
				case FieldType.INT :
					groupingArrayData.setIntegerValue(
							fieldIndexInfo.getIndex(), raf.read_int());
					break;
				case FieldType.LONG :
					groupingArrayData.setLongValue(fieldIndexInfo.getIndex(),
							raf.read_long());
					break;
				case FieldType.FLOAT :
					groupingArrayData.setFloatValue(fieldIndexInfo.getIndex(),
							raf.read_float());
					break;
				case FieldType.DOUBLE :
					groupingArrayData.setDoubleValue(fieldIndexInfo.getIndex(),
							raf.read_double());
					break;
				case FieldType.STRING :
					groupingArrayData.setStringValue(fieldIndexInfo.getIndex(),
							raf.read_string());
					break;
				case FieldType.DATE :
					groupingArrayData.setDateValue(fieldIndexInfo.getIndex(),
							new Date(raf.read_long()));
					break;
				default :
					throw new IllegalArgumentException("类型参数" + type + "不正确");
			}
		}
	}

	// for test
	/**
	 * public static void main(String[] args) throws Exception { Calendar
	 * calendar = Calendar.getInstance(); calendar.set(Calendar.MONTH, 2);
	 * calendar.set(Calendar.DAY_OF_MONTH, 3);
	 * calendar.set(Calendar.HOUR_OF_DAY, 15); calendar.set(Calendar.MINUTE, 0);
	 * calendar.set(Calendar.SECOND, 0); calendar.set(Calendar.MILLISECOND, 0);
	 * long mills = calendar.getTimeInMillis();
	 * 
	 * DataSource ds = new DataSource(100, "DAY", new Timestamp(mills) , "8",
	 * "2", "", 300, "START_TIME", "date", "DS_CHECK_NE_PIECE_W", 0, 0);
	 * 
	 * int cityID = 0; List<String> columns = new ArrayList<>(); // from task
	 * columns.add("INTRAFREQ_MR_NUM_D"); columns.add("START_TIME");
	 * columns.add("NE_CELL_ID"); columns.add("CELL_NAME");
	 * columns.add("CITY_ID"); columns.add("DOWNCOVERGOOD_NUM_D");
	 * 
	 * Date date = new Date(mills); System.out.println(date); BlockDataProvider
	 * impl = BlockDataProviderImpl.getInstance();
	 * 
	 * BlockData data = impl.load(ds, cityID, columns, date, null);
	 * GroupingArrayDataDescriptor gadd = data.getMetaInfo(); FieldIndexInfo
	 * fii1 = gadd.getFileIndexInfo("intrafreq_mr_num_d"); FieldIndexInfo fii2 =
	 * gadd.getFileIndexInfo("downcovergood_num_d"); FieldIndexInfo fii3 =
	 * gadd.getFileIndexInfo("ne_cell_id");
	 * 
	 * Map<String, GroupingArrayData> gads = data.getGroupingArrayDatas();
	 * 
	 * for (GroupingArrayData gad : gads.values()) { Object obj1 =
	 * gad.getPropertyValue(fii1); Object obj2 = gad.getPropertyValue(fii2);
	 * Object obj3 = gad.getPropertyValue(fii3); if
	 * (!"2010100065059022".equals(obj3)) continue;
	 * 
	 * System.out.println("intrafreq_mr_num_d:" + obj1);
	 * System.out.println("downcovergood_num_d:" + obj2); } }
	 */

}

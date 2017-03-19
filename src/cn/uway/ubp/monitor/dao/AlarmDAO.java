package cn.uway.ubp.monitor.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.rule.Alarm;
import cn.uway.ubp.monitor.rule.AlarmDetail;

/**
 * 告警输出DAO
 * 
 * @author liuchao @ 2013-12-02
 */
public class AlarmDAO extends DAO {

	private static final AlarmDAO DAO = new AlarmDAO();

	private static final Logger logger = LoggerFactory
			.getLogger(AlarmDAO.class);

	public static AlarmDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 获取入库的alarm_id序列号
	 * 
	 * @return 序列号
	 * @throws SQLException
	 * </pre>
	 */
	public Long getAlarmIDSeq(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select UBP_MONITOR_TASK_ALARM_SEQ.Nextval from dual";
		Long alarmId = null;

		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			if (rs.next())
				alarmId = rs.getLong(1);
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return alarmId;
	}
	
	/**
	 * 告警入库（主表+详表）
	 * @param alarmList 告警列表
	 * @param keyIndexType
	 * @param tId
	 * @param detailEnable 是否入库详表
	 * @return 返回入库主表的数量
	 * @throws SQLException
	 * @throws ParseException
	 */
	public int save(Connection conn, List<Alarm> alarmList, Integer keyIndexType,
			Integer tId, boolean detailEnable) throws SQLException, ParseException {
		int count = 0;
		
		try {
			conn.setAutoCommit(false);
			
			count = saveAlarm(conn, alarmList, keyIndexType, tId);
			
			if (detailEnable) {
				saveAlarmDetail(conn, alarmList);
			}
			
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		} 
		
		return count;
	}

	/**
	 * <pre>
	 * 存储数据告警插入数据库成功返回true
	 * 1.多数据记录批处理
	 * 2.失败数据回滚
	 * 
	 * @param conn
	 * @param alarmList
	 * @param keyIndexType
	 * @param tId
	 * @throws SQLException
	 * @throws ParseException
	 * @return 告警入库成功数量
	 * </pre>
	 */
	private int saveAlarm(Connection conn, List<Alarm> alarmList, Integer keyIndexType,
			Integer tId) throws SQLException, ParseException {
		PreparedStatement pstmt = null;
		String sql = "insert into UBP_MONITOR_ALARM(ALARM_ID,ALARM_TIME,CITY_ID,NE_LEVEL,TITLE_TEXT,ALARM_TEXT,ALARM_LEVEL,START_TIME,END_TIME,TASK_ID,MONITOR_TASK_ID,IS_CLEAR,NE_NAME,KEY_INDEX,KEY_INDEX_TYPE,T_ID,MONITOR_VALUE,NE_TYPE,C0,C1,C2,C3,C4,C5,C6,C7,C8,C9)"
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		int exportNum = 0;

		try {
			pstmt = conn.prepareStatement(sql);
			for (Alarm alarm : alarmList) {
				Timestamp alarmTime = new Timestamp(DateTimeUtil.parseDateTime(
						alarm.getData().get("ALARM_TIME")).getTime());
				double cityId = Double.valueOf(alarm.getData().get("CITY_ID"));
				short alarmLevel = Short.parseShort(alarm.getData().get(
						"ALARM_LEVEL"));
				Timestamp startTime = new Timestamp(DateTimeUtil.parseDateTime(
						alarm.getData().get("START_TIME")).getTime());
				Timestamp endTime = new Timestamp(DateTimeUtil.parseDateTime(
						alarm.getData().get("END_TIME")).getTime());
				try {

					Integer monitorId = Integer.valueOf(alarm.getData().get(
							"MONITOR_ID"));
					Integer isClear = Integer.valueOf(alarm.getData().get(
							"IS_CLEAR"));
					Integer _keyIndexType = keyIndexType == null
							? 0
							: keyIndexType;
					Integer _tId = tId == null ? 0 : tId;
					Double monitorValue = "null".equals(alarm.getData().get(
							"MONITOR_VALUE")) ? 0 : Double.valueOf(alarm
							.getData().get("MONITOR_VALUE"));

					pstmt.setLong(1, alarm.getAlarmID());
					pstmt.setTimestamp(2, alarmTime);
					pstmt.setDouble(3, cityId);
					String ne_level = alarm.getData().get("NE_LEVEL");
					pstmt.setString(4, ne_level);
					String title_text = alarm.getData().get("TITLE_TEXT");
					pstmt.setString(5, title_text);

					final int MAX_ALARM_TEXT_LENGTH = 4000;
					String charset = Configuration
							.getString(Configuration.ALARM_CHARSET);
					byte[] bs = alarm.getData().get("ALARM_TEXT")
							.getBytes(charset);
					String text = new String(bs, 0, Math.min(bs.length,
							MAX_ALARM_TEXT_LENGTH));
					pstmt.setString(6, text);

					pstmt.setShort(7, alarmLevel);
					pstmt.setTimestamp(8, startTime);
					pstmt.setTimestamp(9, endTime);
					pstmt.setInt(10, monitorId);
					String monitor_task_id = alarm.getData().get(
							"MONITOR_TASK_ID");
					pstmt.setString(11, monitor_task_id);
					pstmt.setInt(12, isClear);
					String ne_name = alarm.getData().get("NE_NAME");
					pstmt.setString(13, ne_name);
					String ne_sys_id = alarm.getData().get("NE_SYS_ID");
					pstmt.setString(14, ne_sys_id);
					pstmt.setInt(15, _keyIndexType);
					pstmt.setInt(16, _tId);
					pstmt.setDouble(17, monitorValue);
					String ne_type = alarm.getData().get("NE_TYPE");
					pstmt.setString(18, ne_type);

					// memo
					pstmt.setString(19, alarm.getData().get("C0"));
					pstmt.setString(20, alarm.getData().get("C1"));
					pstmt.setString(21, alarm.getData().get("C2"));
					pstmt.setString(22, alarm.getData().get("C3"));
					pstmt.setString(23, alarm.getData().get("C4"));
					pstmt.setString(24, alarm.getData().get("C5"));
					pstmt.setString(25, alarm.getData().get("C6"));
					pstmt.setString(26, alarm.getData().get("C7"));
					pstmt.setString(27, alarm.getData().get("C8"));
					pstmt.setString(28, alarm.getData().get("C9"));

					Object[] parameters = new Object[]{alarm.getAlarmID(),
							alarmTime, cityId, alarm.getData().get("NE_LEVEL"),
							alarm.getData().get("TITLE_TEXT"), text,
							alarmLevel, startTime, endTime, monitorId,
							alarm.getData().get("MONITOR_TASK_ID"), isClear,
							alarm.getData().get("NE_NAME"),
							alarm.getData().get("NE_SYS_ID"), _keyIndexType,
							_tId, monitorValue, alarm.getData().get("NE_TYPE"),
							alarm.getData().get("C0"),
							alarm.getData().get("C1"),
							alarm.getData().get("C2"),
							alarm.getData().get("C3"),
							alarm.getData().get("C4"),
							alarm.getData().get("C5"),
							alarm.getData().get("C6"),
							alarm.getData().get("C7"),
							alarm.getData().get("C8"),
							alarm.getData().get("C9")};

					log(logger, conn, sql, parameters);

					exportNum++;
					pstmt.addBatch();
					if ((exportNum % 200 == 0 && exportNum != 0)
							|| (exportNum == alarmList.size())) {
						pstmt.executeBatch();
						pstmt.clearBatch();
					}
				} catch (Exception e) {
					err(logger, conn, sql, e, new Object[]{alarm.getAlarmID(), alarmTime, cityId,
									alarm.getData().get("NE_LEVEL"),
									alarm.getData().get("TITLE_TEXT"),
									alarm.getData().get("ALARM_TEXT"),
									alarmLevel, startTime, endTime,
									alarm.getData().get("MONITOR_ID"),
									alarm.getData().get("MONITOR_TASK_ID"),
									alarm.getData().get("IS_CLEAR"),
									alarm.getData().get("NE_NAME"),
									alarm.getData().get("NE_SYS_ID"),
									keyIndexType, tId,
									alarm.getData().get("MONITOR_VALUE"),
									alarm.getData().get("NE_TYPE"),
									alarm.getData().get("C0"),
									alarm.getData().get("C1"),
									alarm.getData().get("C2"),
									alarm.getData().get("C3"),
									alarm.getData().get("C4"),
									alarm.getData().get("C5"),
									alarm.getData().get("C6"),
									alarm.getData().get("C7"),
									alarm.getData().get("C8"),
									alarm.getData().get("C9")});
					/*
					logger.warn(
							"告警入库失败，AlarmId：{}，AlarmTime：{}，CityId：{}，NE_Level：{}，Title_Text：{}，Alarm_Text：{}"
									+ "， AlarmLevel：{}，StartTime：{}，EndTime：{}，MonitorId：{}，Monitor_Task_Id：{}，Is_Alarm_Clear：{}"
									+ "，NE_Name：{}，NE_Sys_Id：{}，KeyIndexType：{}，TId：{}，MonitorValue：{}，NE_Type：{}，C0：{}，C1：{}"
									+ "，C2：{}，C3：{}，C4：{}，C5：{}，C6：{}，C7：{}，C8：{}，C9：{}",
							new Object[]{alarm.getAlarmID(), alarmTime, cityId,
									alarm.getData().get("NE_LEVEL"),
									alarm.getData().get("TITLE_TEXT"),
									alarm.getData().get("ALARM_TEXT"),
									alarmLevel, startTime, endTime,
									alarm.getData().get("MONITOR_ID"),
									alarm.getData().get("MONITOR_TASK_ID"),
									alarm.getData().get("IS_CLEAR"),
									alarm.getData().get("NE_NAME"),
									alarm.getData().get("NE_SYS_ID"),
									keyIndexType, tId,
									alarm.getData().get("MONITOR_VALUE"),
									alarm.getData().get("NE_TYPE"),
									alarm.getData().get("C0"),
									alarm.getData().get("C1"),
									alarm.getData().get("C2"),
									alarm.getData().get("C3"),
									alarm.getData().get("C4"),
									alarm.getData().get("C5"),
									alarm.getData().get("C6"),
									alarm.getData().get("C7"),
									alarm.getData().get("C8"),
									alarm.getData().get("C9")}, e);
					*/
					// 这里不往上抛异常的原因是，为了不重复添加告警信息时，是通过主键的唯一性来实现的
					// 为了不给上层打出多余的日志，这里就没有往上抛，只在dao中记录
				}
			}
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return exportNum;
	}

	/**
	 * <pre>
	 * 存储数据告警插入数据库成功返回true
	 * 1.多数据记录批处理
	 * 2.失败数据回滚
	 * 
	 * @param conn
	 * @param alarmList
	 * @return
	 * @throws SQLException
	 * @throws ParseException
	 * @return 告警入库成功数量
	 * </pre>
	 */
	private int saveAlarmDetail(Connection conn, List<Alarm> alarmList) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "insert into UBP_MONITOR_ALARM_DETAIL(ALARM_ID,ALARM_TIME,DATA_TIME,ALARM_COUNT1,ALARM_COUNT2,ALARM_COUNT3,ALARM_COUNT4,ALARM_COUNT5,ALARM_COUNT6)"
				+ " values(?,?,?,?,?,?,?,?,?)";
		int sumExportNum = 0;
		try {
			pstmt = conn.prepareStatement(sql);

			for (Alarm alarm : alarmList) {
				Object[] parameters = null;
				try {
					Timestamp alarmTime = new Timestamp(DateTimeUtil
							.parseDateTime(alarm.getData().get("ALARM_TIME"))
							.getTime());
					pstmt.setLong(1, alarm.getAlarmID());
					pstmt.setTimestamp(2, alarmTime);

					List<AlarmDetail> alarmDetails = alarm.getDetailDatas();
					int exportNum = 0;
					for (AlarmDetail alarmDetail : alarmDetails) {
						Timestamp alarmDatetime = new Timestamp(alarmDetail
								.getDatetime().getTime());
						pstmt.setTimestamp(3, alarmDatetime);
						parameters = new Object[]{alarm.getAlarmID(),
								alarmTime, alarmDatetime, "", "", "", "", "",
								""};

						Map<String, Object> indicatorValues = alarmDetail
								.getIndicatorValues();
						Iterator<Entry<String, Object>> iterIndicator = indicatorValues
								.entrySet().iterator();
						int indicatorCount = 0;
						int fieldIndex = 4;
						StringBuilder sb = new StringBuilder();
						sb.append("{");
						while (iterIndicator.hasNext()) {
							// {"掉话率": "30" , "次数": "150"}
							Entry<String, Object> entry = iterIndicator.next();
							sb.append('\"').append(entry.getKey()).append('\"');
							sb.append(':');
							sb.append('\"').append(entry.getValue())
									.append('\"');
							sb.append(',');

							if (++indicatorCount == 40
									|| !iterIndicator.hasNext()) {
								indicatorCount = 0;
								// 删掉最后一个","号；
								sb.deleteCharAt(sb.length() - 1);
								// 添加闭合大括号
								sb.append("}");

								// 设置指标值
								String content = sb.toString();
								pstmt.setString(fieldIndex, content);
								parameters[fieldIndex - 1] = content;

								// 字段索引加1
								++fieldIndex;
								// 清空字符串
								sb.setLength(0);
								// 添加首字符串
								sb.append("{");
							}
						}

						while (fieldIndex < 10) {
							pstmt.setString(fieldIndex++, "");
						}

						log(logger, conn, sql, parameters);
						pstmt.addBatch();
						exportNum++;
						if ((exportNum % 500 == 0 && exportNum != 0)
								|| (exportNum == alarmDetails.size())) {
							pstmt.executeBatch();
							pstmt.clearBatch();
							sumExportNum += exportNum;
						}
					}
				} catch (Exception e) {
					err(logger, conn, sql, e);
				}
			}
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return sumExportNum;
	}

}

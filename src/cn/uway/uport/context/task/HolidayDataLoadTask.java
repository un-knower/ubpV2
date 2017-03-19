package cn.uway.uport.context.task;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.uport.dao.HolidayDAO;
import cn.uway.util.HolidayChecker;

/**
 * 节假日数据载入任务
 * 
 * @author liuchao
 * @ 2014年5月5日
 */
public class HolidayDataLoadTask implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(HolidayDataLoadTask.class);

	@Override
	public void run() {
		try {
			Set<Long> set = new HashSet<>();

			HolidayDAO dao = HolidayDAO.getInstance();
			List<Timestamp> holidayList = dao.getHolidays();

			if (holidayList == null)
				throw new Exception("节假日数据无记录");

			for (Timestamp timestamp : holidayList) {
				set.add(timestamp.getTime());
			}

			HolidayChecker.refreshData(set);
		} catch (Exception e) {
			logger.error("刷新节假日数据失败", e);
		}
	}

}

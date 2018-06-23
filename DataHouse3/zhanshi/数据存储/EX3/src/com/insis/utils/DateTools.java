package com.insis.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTools {
	public static SimpleDateFormat format = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/**
	 * 计算耗费时间
	 * 
	 * @param escapeTime
	 */
	public static void calEscapeTime(long escapeTime) {
		long seconds = escapeTime / 1000;
		long minutes = seconds / 60;
		long h = minutes / 60;
		long s = seconds % 60;
		long m = minutes % 60;

		System.out.println("escape time: " + h + ":" + m + ":" + s);
	}

	/**
	 * 
	 * @param sTime
	 *            - 原始时间
	 * @param time
	 *            - 要加的时间
	 * @param interval
	 *            - 时间粒度，秒还是小时
	 * @return
	 */
	public static String addDateMinut(String sTime, int time, String interval) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse(sTime);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (date == null){
			return "";}
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		if (Config.HOUR.equals(interval)) {
			cal.add(Calendar.HOUR, time);// 24小时制
		}
		if (Config.SECOND.equals(interval)) {
			cal.add(Calendar.SECOND, time);// 24小时制
		}
		date = cal.getTime();
		cal = null;
		return sdf.format(date);

	}

	public static void main(String[] args) {
		String s = addDateMinut("2015-01-01 12:00:40", 1, Config.HOUR);
		System.out.println(s);
	}
}

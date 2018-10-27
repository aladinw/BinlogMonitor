package com.weiloong.main.binlog.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtils {

	public static final int PATTERN_DATE = 0;
	public static final int PATTERN_TIME = 1;
	public static final int PATTERN_DATETIME = 2;
	public static final int PATTERN_DATETIME1 = 3;
	public static final int PATTERN_DATETIME2 = 4;
	
	public static final String DATE_FORMAT = "yyyy-MM-dd"; 
	public static final String DATE_FORMAT_1 = "yyyyMM"; 
	public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"; 
	public static final String DATE_MONTH_FORMAT = "yyyy-MM"; 
	
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
	private static DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static DateFormat dateTimeFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss DDD");
	private static DateFormat dateTimeFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private static DateFormat dateTimeFormat3 = new SimpleDateFormat("yyyy-MM");
	private static DateFormat dateTimeFormat4 = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	private static DateFormat yearFormate = new SimpleDateFormat("yyyy");

	private static DateFormat[] formats = { dateFormat, timeFormat,
			dateTimeFormat, dateTimeFormat1, dateTimeFormat2, dateTimeFormat3,
			dateTimeFormat4, yearFormate };

	public static String dateAdd(int days){
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, days); 
		SimpleDateFormat sdf=new SimpleDateFormat(DATE_FORMAT);
		return sdf.format(calendar.getTime());
	}
	
	public static Date dateMonthAdd(int months){
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, months);
		return calendar.getTime();
	}
	
	public static String dateMonthAdd(int months, String format){
		return parseDate(dateMonthAdd(months), format);
	}
	
	public static String dateMonthFormatAdd(int months){
		return parseDate(dateMonthAdd(months), DATE_FORMAT);
	}
	
	public static String convertToString(Date aDate) {
		if (aDate == null) {
			return "";
		}
		return formats[0].format(aDate);
	}

	public static String convertToString1(Date aDate) {
		if (aDate == null) {
			return "";
		}
		return formats[2].format(aDate);
	}

	public static String convertToString2(String aDate) {
		if ((aDate == null) || (aDate.equals(""))) {
			return "";
		}
		String date = aDate;
		String[] nums = date.split("\\-");
		nums[0] = (nums[0] + "年");
		if (nums[1].startsWith("0")) {
			nums[1] = nums[1].substring(1, nums[1].length());
		}
		if (nums[2].startsWith("0")) {
			nums[2] = nums[2].substring(1, nums[2].length());
		}
		nums[1] = (nums[1] + "月");
		nums[2] = (nums[2] + "日");
		return nums[0] + nums[1] + nums[2];
	}

	public static String convertDateToString(Date aDate) {
		if (aDate == null) {
			return "";
		}
		String date = formats[5].format(aDate);
		String[] nums = date.split("\\-");
		nums[0] = (nums[0] + "年");
		if (nums[1].startsWith("0")) {
			nums[1] = nums[1].substring(1, nums[1].length());
		}
		nums[1] = (nums[1] + "月");
		return nums[0] + nums[1];
	}

	public static String convertToCnString(Date aDate) {
		if (aDate == null) {
			return "";
		}
		String date = formats[0].format(aDate);
		String[] nums = date.split("\\-");
		nums[0] = (nums[0] + "年");
		if (nums[1].startsWith("0")) {
			nums[1] = nums[1].substring(1, nums[1].length());
		}
		if (nums[2].startsWith("0")) {
			nums[2] = nums[2].substring(1, nums[2].length());
		}
		nums[1] = (nums[1] + "月");
		nums[2] = (nums[2] + "日");
		return nums[0] + nums[1] + nums[2];
	}

	public static String convertToCnYmString(Date aDate) {
		if (aDate == null) {
			return "";
		}
		String date = formats[5].format(aDate);
		String[] nums = date.split("\\-");
		nums[0] = (nums[0] + "年");
		if (nums[1].startsWith("0")) {
			nums[1] = nums[1].substring(1, nums[1].length());
		}
		nums[1] = (nums[1] + "月");
		return nums[0] + nums[1];
	}

	public static String convertToString(int pattern, Date aDate) {
		if (aDate == null) {
			return "";
		}
		return formats[getPatternIndex(pattern)].format(aDate);
	}

	public static String convertToString(int pattern, long lMili) {
		if (lMili == 0L) {
			return "";
		}
		return formats[getPatternIndex(pattern)].format(Long.valueOf(lMili));
	}

	public static String convertToString(String pattern, Date aDate) {
		if (aDate == null) {
			return "";
		}
		return new SimpleDateFormat(pattern).format(aDate);
	}

	public static Date convertToDate(String strDate) throws ParseException {
		return formats[0].parse(strDate);
	}

	public static synchronized Date convertToDateThreadSafe(String strDate)
			throws ParseException {
		return formats[0].parse(strDate);
	}

	public static synchronized String convertToStringThreadSafe(Date aDate) {
		if (aDate == null) {
			return "";
		}
		return formats[0].format(aDate);
	}

	public static Date convertToDate(int pattern, String strDate)
			throws ParseException {
		return formats[getPatternIndex(pattern)].parse(strDate);
	}

	public static Date convertToDate(String pattern, String strDate)
			throws ParseException {
		return new SimpleDateFormat(pattern).parse(strDate);
	}

	public static Date convert2Date(String pattern, String strDate) {
		try {
			return new SimpleDateFormat(pattern).parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static int getMaxDay(int year, int month) {
		if (isLongMonth(month)) {
			return 31;
		}
		if (isShortMonth(month)) {
			return 30;
		}

		if (isLeapYear(year)) {
			return 29;
		}

		return 28;
	}

	private static boolean isLongMonth(int month) {
		return (month == 1) || (month == 3) || (month == 5) || (month == 7)
				|| (month == 8) || (month == 10) || (month == 12);
	}

	private static boolean isShortMonth(int month) {
		return (month == 4) || (month == 6) || (month == 9) || (month == 11);
	}

	private static boolean isLeapYear(int year) {
		return (year % 400 == 0) || ((year % 4 == 0) && (year % 100 != 0));
	}

	private static int getPatternIndex(int pattern) {
		if (pattern > formats.length) {
			return 0;
		}
		return pattern;
	}

	public static String convertToYearString(Date aDate) {
		return convertToString("yyyy", aDate);
	}

	public static int returnYear(Date aDate) {
		String date = formats[0].format(aDate);
		String[] nums = date.split("\\-");
		int year = new Integer(nums[0]).intValue();
		return year;
	}

	public static int convertToDayOfWeek(Date aDate) {
		Calendar c = new GregorianCalendar();
		c.setTime(aDate);
		return c.get(7) - 1;
	}

	public static final int getMonthsDifference(Date beginningDate,
			Date endingDate) {
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.setTime(beginningDate);
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.setTime(endingDate);

		int diffYear = endCalendar.get(1) - startCalendar.get(1);
		int diffMonth = diffYear * 12
				+ (endCalendar.get(2) - startCalendar.get(2));
		return diffMonth;
	}

	public static final int getMonthsDifferenceInMonth(Date beginningDate,
			Date endingDate) {
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.setTime(beginningDate);
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.setTime(endingDate);
		int startDay = startCalendar.get(5);
		int endDay = endCalendar.get(5);
		int diffYear = endCalendar.get(1) - startCalendar.get(1);
		int diffMonth = diffYear * 12
				+ (endCalendar.get(2) - startCalendar.get(2));
		if (startDay > endDay) {
			diffMonth--;
		}
		return diffMonth;
	}

	public static final int getDaydifference(Date beginningDate, Date endingDate) {
		long diff = endingDate.getTime() - beginningDate.getTime();
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}

	public static Date parseDate(String text) {
		Date date = null;
		if ((text == null) || (text.isEmpty()))
			return null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		try {
			date = dateFormat.parse(text);
		} catch (ParseException e) {
			return null;
		}
		return date;
	}

	public static Date parseDate(String text, String pattern) {
		Date date = null;
		if ((text == null) || (text.isEmpty()) || (pattern == null)
				|| (pattern.isEmpty()))
			return null;
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		try {
			date = dateFormat.parse(text);
		} catch (ParseException e) {
			return null;
		}
		return date;
	}

	public static String parseDate(Date date) {
		if (date == null)
			return "";
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		return dateFormat.format(date);
	}

	public static String parseDate(Date date, String pattern) {
		if (date == null)
			return "";
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		return dateFormat.format(date);
	}
	
	public static Date getCurrentDate() {
		return DateUtils.parseDate(DateUtils.getCurrentDateStr(),DateUtils.DATE_FORMAT);
	}
	
	public static Date getCurrentTime() {
		return parseDate(parseDate(new Date()));
	}
	
	public static String getCurrentDateStr(){
		return getCurrentTimeStr(DATE_FORMAT);
	}
	
	public static String getCurrentTimeStr(){
		return getCurrentTimeStr(DATETIME_FORMAT);
	}
	
	public static String getCurrentTimeStr(String dateFormat){
		return parseDate(new Date(),dateFormat);
	}

	public static String getMissionDate(Date date, int days) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(5, days);
		Date missionDate = calendar.getTime();
		return convertToString(missionDate);
	}

	public static Date getRegulateDate(Calendar cal) {
		int year = cal.get(1);
		int month = cal.get(2) + 1;
		if (month == 12) {
			month = 1;
			year++;
		} else {
			month++;
		}
		String strDate = year + "-";
		if (month >= 10)
			strDate = strDate + month + "-";
		else {
			strDate = strDate + "0" + month + "-";
		}
		strDate = strDate + "28";
		Date date = null;
		try {
			date = convertToDate(strDate);
		} catch (Exception e) {
			return null;
		}
		return date;
	}

	public static int getDayOfMonth() {
		return Calendar.getInstance().get(5);
	}

	public static final int getDaydifferenceByGHB(Date dtRepayDate,
			Date dtCurrDate) {
		Calendar c = Calendar.getInstance();
		c.setTime(dtRepayDate);
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		c.add(2, -1);
		String format = f.format(c.getTime());
		Date beforeDate = parseDate(format, "yyyy-MM-dd");
		long diff = dtCurrDate.getTime() - beforeDate.getTime();
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * 获取一个月最后一天或者第一天带时分秒字符串
	 * 
	 * @param month 何年何月，注意该参数格式和格式化形式保持一致
	 * @param monthFormat 格式化形式
	 * @param isLast true 为最后一天，false为第一天
	 * @return yyyy#MM#dd HH:mm:ss #为格式化分割符
	 */
	public static String getDayofMonth(String month, String monthFormat, boolean isLast){
		
		Calendar calendar = Calendar.getInstance();
		Date d = parseDate(month, monthFormat);
		calendar.setTime(d);
		int day = 32;
		
		if(isLast){
			day = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
		}else{
			day = calendar.getActualMinimum(Calendar.DAY_OF_MONTH);
		}
		StringBuilder sb = new StringBuilder();
		
		String regex = "\\W";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(month);
		if(!m.find()){
			throw new RuntimeException("未获取到时间分隔符");
		}
		String str = m.group();
		sb.append(month).append(str).append(String.format("%02d", day));
		if(isLast){
			sb.append(" 23:59:59");
		}else{
			sb.append(" 00:00:00");
		}
		return sb.toString();
	}

	/**
	 * 获取一个月最后一天或者第一天带时分秒字符串
	 * @param month 只支持 yyyy-MM 格式参数
	 * @return yyyy-MM-dd HH:mm:ss
	 */
	public static String getDayofMonth(String month, boolean isLast){
		return getDayofMonth(month, DATE_MONTH_FORMAT, isLast);
	}
	
	/**
	 * 获取一个月的范围字符串数组，从 yyyy-MM-01 00:00:00 到 yyyy-MM-28/29/30/31 23:59:59
	 * @param date
	 * @return 0角标为第一天，1角标为最后一天；
	 */
	public static String[] getOneMonthRange(Date date){
		String m = parseDate(date, DATE_MONTH_FORMAT);
		String firstDate = getDayofMonth(m, false);
		String lastDate = getDayofMonth(m, true);
		
		String[] dRange = {firstDate,lastDate};
		return dRange;
	}
}

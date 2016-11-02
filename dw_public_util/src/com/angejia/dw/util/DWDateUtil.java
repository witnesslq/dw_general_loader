package com.angejia.dw.util;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;

import com.angejia.dw.exception.ParseRuntimeException;
import com.angejia.dw.util.DWDateUtil;
import com.angejia.dw.util.DateHisType;

/**
 * 一些DW常用的日期解析与计算方法
 * @author larrysun
 *
 */
public class DWDateUtil
{
    //SimpleDateFormat线程不安全！！
    private static final String DATE_FORMAT = "yyyy-MM-dd";
   
    /**
     * 返回"昨天"的字符串表示
     * @return 昨天的字符串表示
     */
    public static String getYesterDay()
    {
        Date yesterDay = DateUtils.addDays(new Date(), -1);
        return DateFormatUtils.format(yesterDay, DATE_FORMAT);
    }
    
    public static String getToday()
    {
        return DateFormatUtils.format(new Date(), DATE_FORMAT);
    }
    
    /**
     * 由字符串解析Date，如果格式不满足 yyyy-MM-dd，则抛出异常
     * @param day  yyyy-MM-dd形式的日期
     * @return Date类型的日期
     */
    public static Date parseDate(String day)
    {
        try
        {
           return DateUtils.parseDate(day, new String[]{DATE_FORMAT});
        }
        catch (ParseException e)
        {
            throw new ParseRuntimeException(e);
        }
    }
    
    /**
     * 返回date的字符串形式
     * @param date Date类型的日期
     * @return  yyyy-MM-dd形式的日期
     */
    public static String formatDate(Date date)
    {
        return DateFormatUtils.format(date, DATE_FORMAT);
    }
    
    /**
     * 增加/减少日期天数
     * @param day yyyy-MM-dd形式的日期
     * @param count 增加或减少的天数（负数时为减少）
     * @return 增加或减少后的日期
     */
    public static String addDays(String day, int count)
    {
        Date date = parseDate(day);
        Date newDate = DateUtils.addDays(date, count);
        return formatDate(newDate);
    }
    
    /**
     * 增加/减少日期月数
     * @param day yyyy-MM-dd形式的日期
     * @param count 增加或减少的月数（负数时为减少）
     * @return 增加或减少后的日期
     */
    public static String addMonths(String day, int count)
    {
        Date date = parseDate(day);
        Date newDate = DateUtils.addMonths(date, count);
        return formatDate(newDate);
    }
    
    /**
     * 以周一为一周第一天，周末为最后一天，返回给定日期所在周的周一
     * @param date 输入日期
     * @return 给定日期所在周的周一
     */
    public static Date getMondayOfTheWeek(Date date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
        int minus = dayOfWeek - 2;
        c.add(Calendar.DATE, -minus);

        if (minus < 0) // 说明这天是星期天，还要再减去一周
            c.add(Calendar.DATE, -7);

        return c.getTime();
    }
    
    /**
     * 以周日为一周第一天，周六为最后一天，返回给定日期所在周的周六
     * @param date 输入日期
     * @return 给定日期所在周的周六
     */
    public static Date getSaturdayOfTheWeek(Date date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
        int minus = 7 - dayOfWeek ;
        c.add(Calendar.DATE, minus);
        return c.getTime();
    }
    
    /**
     * 以周一为一周第一天，周日为最后一天，返回给定日期所在周的周日
     * @param date 输入日期
     * @return 给定日期所在周的周日
     */
    public static Date getSundayOfTheWeek(Date date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
        int minus = 1-dayOfWeek ;
        c.add(Calendar.DATE, minus);
        return c.getTime();
    }
    
    public static Date getSundayOfTheWeek(String date)
    {
        Date d = parseDate(date);
        return getSundayOfTheWeek(d);
    }
    
    public static Date getSaturdayOfTheWeek(String date)
    {
        Date d = parseDate(date);
        return getSaturdayOfTheWeek(d);
    }
    
    /**
     * 以周一为一周第一天，周末为最后一天，返回给定日期所在周的周一
     * @param date yyyy-MM-dd输入日期
     * @return 给定日期所在周的周一
     */
    public static Date getMondayOfTheWeek(String date)
    {
        Date d = parseDate(date);
        return getMondayOfTheWeek(d);
    }
    
    /**
     * 判断是星期几
     * @param date
     * @return
     */
    public static int getDayOfWeek(String date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(DWDateUtil.parseDate(date));
        return c.get(Calendar.DAY_OF_WEEK);
    }
    
    /**
     * 判断是今年的第几周
     * @param date
     * @return
     */
    public static int getWeekOfYear(Date date)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.WEEK_OF_YEAR);
    }
    
    /**
     * 是否月末，做月表时可用
     * @param date
     * @return
     */
    public static boolean isLastDayOfMonth(String date)
    {
        return addDays(date, 1).substring(8).equals("01");
    }
    
    /**
     * 根据日期返回所属月份
     * @param withM 是否用字母"M"隔开年份和月份
     * @return 月份如201001或2010M01
     */
    public static String getMonthId(String date, boolean withM)
    {
        return date.substring(0, 4) + (withM ? "M" : "") + date.substring(5, 7);
    }
    
    /**
     * 根据日期返回所属月份,不带年,2014-06-30 ->06
     */
    public static String getMonthOnly(String date){
    	String monthId = getMonthId(date,false);
    	return monthId.substring(4);
    }
    
    /**
     * 根据日期返回所属月份第一天
     * @param 当前日期
     * @return 本月第一天
     */
    public static String getMonthBegin(String date)
    {
        return date.substring(0, 7) + "-01";
    }
    
    /**
     * 根据日期返回所属月份最后一天
     * @param 当前日期
     * @return 本月最后一天
     */
    @SuppressWarnings("deprecation")
	public static String getMonthEnd(String date)
    {
    	Date d = parseDate(date);
    	Calendar c = Calendar.getInstance();
        c.setTime(d);
        int lastDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
        Date   lastDate = c.getTime();
        lastDate.setDate(lastDay);
        return   DateFormatUtils.format(lastDate, DATE_FORMAT);
    }
    
    /**
     * 按照一周分界线返回日期类型（昨天，七天内，七天前），用于计算七天返回率
     * @param dealDate
     * @return
     */
    public static DateHisType getDateHisType(String dealDate)
    {
        String yesterday = DWDateUtil.getYesterDay();
        if ( yesterday.equals(dealDate) )
            return DateHisType.YESTERDAY;
        else if ( DateUtils.addDays(DWDateUtil.parseDate(yesterday), -7)
                         .compareTo(DWDateUtil.parseDate(dealDate)) >= 0 )
            return DateHisType.BEFORE_7DAY;
        else return DateHisType.IN_7DAY;
    }
    
    /**
     * 获取这周第一天，周日，如果跨年，获取这年一月一号
     * @param dateStr
     * @return
     */
    @SuppressWarnings("deprecation")
	public static String getWeekBeginDtEn(String dateStr){
    	Date sunday = getSundayOfTheWeek(dateStr);
    	Calendar g = Calendar.getInstance();
    	String weekBegindt= null;
    	try {
    		Date dayDate = DateUtils.parseDate(dateStr, new String[]{DATE_FORMAT});
			g.setTime(dayDate);
			//判断当天这周有跨年
			if(sunday.getYear()< dayDate.getYear()){
				weekBegindt = dateStr.substring(0,5)+"01-01";
			}else{
				weekBegindt = DateFormatUtils.format(sunday, DATE_FORMAT);
			}
			
			return weekBegindt; 
		} catch (ParseException e) {
			throw new ParseRuntimeException(e);
		}
    }
    
    @SuppressWarnings("deprecation")
	public static String getWeekEndDtEn(String dateStr){
    	Date saturday = getSaturdayOfTheWeek(dateStr);
    	Calendar g = Calendar.getInstance();
    	String weekBegindt= null;
    	try {
    		Date dayDate = DateUtils.parseDate(dateStr, new String[]{DATE_FORMAT});
			g.setTime(dayDate);
			//判断当天这周有跨年
			if(saturday.getYear()> dayDate.getYear()){
				weekBegindt = dateStr.substring(0,5)+"12-31";
			}else{
				weekBegindt = DateFormatUtils.format(saturday, DATE_FORMAT);
			}
			
			return weekBegindt; 
		} catch (ParseException e) {
			throw new ParseRuntimeException(e);
		}
    }
    
    @SuppressWarnings("deprecation")
	public static String getWeekIdEn(String dateStr)
	{            
    	Calendar g = Calendar.getInstance();
    	try {
    		Date dayDate = DateUtils.parseDate(dateStr, new String[]{DATE_FORMAT});
			g.setTime(dayDate);
			int weeks =  g.get(Calendar.WEEK_OF_YEAR);
			//判断当天这周有跨年
			if(getSaturdayOfTheWeek(dateStr).getYear()> dayDate.getYear()){
				weeks=getWeekOfYear(DateUtils.addDays(dayDate, -7))+1;
			}
			
			return String.format("%sW%02d", new Object[]{dateStr.substring(0, 4),weeks}); 
		} catch (ParseException e) {
			throw new ParseRuntimeException(e);
		}
	}
}

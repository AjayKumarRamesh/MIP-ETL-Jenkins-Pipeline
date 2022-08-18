package com.ibm.mdm.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtils {

	/**
	 * date format
	 */
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String yyyy_MM_dd = "yyyy-MM-dd";
	public static final String yyyy_MM = "yyyy-MM";
	public static final String dd = "dd";
	public static final String yyyyMMddHHmmssSS="yyyyMMddHHmmssSS";

	/**
	 * @param date
	 *          
	 * @param format
	 *           
	 * @return String
	 */
	public static String getFormatStrFromDate(Date date, String formart) {
		if (date == null) {
			return null;
		}

		SimpleDateFormat dateFormat = new SimpleDateFormat(formart);
		try {
			return dateFormat.format(date);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * @Title: dayByStartAndEnd
	 * @Description:
	 * @param startDate
	 *           
	 * @param toDay
	 *           
	 * @param formart
	 *            
	 * @return 
	 */
	public static List<String> dateStringByDateAndDay(Date startDate, int toDay,
			String formart) {
		List<String> dateStrs = new ArrayList<String>();
		Calendar dateCal = Calendar.getInstance();
		dateCal.setTime(startDate);
		dateStrs.add(DateUtils.getFormatStrFromDate(startDate, formart));

		for (int i = 0; i < toDay; i++) {
			dateCal.add(Calendar.DATE, 1);
			dateStrs.add(DateUtils.getFormatStrFromDate(dateCal.getTime(), formart));
		}

		return dateStrs;
	}
	
	
	/**
	 * @Title: topMonthDate
	 * @Description:
	 * @param @param date
	 * @param @param today
	 * @return Date 
	 * @throws 
	 */
	public static Date topMonthDate(Date date,int today){
		Calendar dateCal = Calendar.getInstance();
		dateCal.setTime(date);
		dateCal.add(Calendar.MONTH, today);
		return dateCal.getTime();
	}
	
	/**
	 * @Title: topDayDate
	 * @Description: 
	 * @param @param date
	 * @param @param today
	 * @return Date  
	 * @throws 
	 */
	public static Date topDayDate(Date date,int today){
		Calendar dateCal = Calendar.getInstance();
		dateCal.setTime(date);
		dateCal.add(Calendar.DATE, today);
		return dateCal.getTime();
	}
	
	public static String getRealByDate(Date date) {
		if(date!=null) {
			String hoursAndMinis=DateUtils.getFormatStrFromDate(date,"HH:mm");
			String hour=hoursAndMinis.split(":")[0];
			String mini=hoursAndMinis.split(":")[1];
			int miniInt=Integer.parseInt(mini);
			DecimalFormat df=new DecimalFormat("0.00");
		    String ii=df.format((float)miniInt/60);
		    float result=Float.valueOf(hour)+Float.valueOf(ii);
			return String.valueOf(result);	
		}
		return null;
	}
	
	public static void main(String[] args){
	   //System.out.println(DateUtils.getFormatStrFromDate(new Date(System.currentTimeMillis()),DateUtils.yyyyMMddHHmmssssss));
		Date date=new Date(System.currentTimeMillis());
		System.out.println(DateUtils.getRealByDate(date));
	}

}

package com.angejia.dw.util;

import java.util.Date;

import com.angejia.dw.util.DWDateUtil;

/**
 * 按不同的日期取得对应的月表或历史表
 * @author larrysun
 *
 */
public class TableNameUtil
{
    /**
     * 按日期取得所在的soj月表
     * @param dealDate
     * @return soj表名
     */
    public static String getSojTableName(String dealDate)
    {
        return getMonthlyTableName(dealDate, "dw_soj_imp_dtl");  
    }
    
    /**
     * 按日期取得所在的soj月表
     * @param dealDate
     * @return soj表名
     */
    public static String getSojTableName(Date dealDate)
    {
       return getSojTableName(DWDateUtil.formatDate(dealDate));
    }
    
    public static String getPropClickTableName(String dealDate)
    {
        return getMonthlyTableName(dealDate, "dw_propclicks_daily");  
    }
    
    public static String getPropClickTableName(Date dealDate)
    {
        return getPropClickTableName(DWDateUtil.formatDate(dealDate));
    }
    
    public static String getInfoWindowTableName(String dealDate)
    {
        return getMonthlyTableName(dealDate, "dw_infowindow_dtl");  
    }
    
    public static String getInfoWindowTableName(Date dealDate)
    {
        return getInfoWindowTableName(DWDateUtil.formatDate(dealDate));
    }
    
    public static String getSojMonthTableName(String dealDate)
    {
        return "dw_soj_imp_dtl_" + dealDate.substring(0, 4) + dealDate.substring(5, 7);
    }
    
    public static String getUbaMonthTableName(String dealDate)
    {
        return "dw_web_visit_traffic_log" + dealDate.substring(0, 4) + dealDate.substring(5, 7);
    }
    
    
    /**
     * 通用的取月表方法
     * @param dealDate
     * @param tableNameDaily 日表表名
     * @return
     */
    public static String getMonthlyTableName(String dealDate, String tableNameDaily)
    {
        String tableName = tableNameDaily;
        if (!dealDate.equals(DWDateUtil.getYesterDay()))
            tableName = tableNameDaily + "_" + dealDate.substring(0, 4) + dealDate.substring(5, 7);

        return tableName;
    }
    
    public static String getSojTableNameForIB(String dealDate)
    {
        return "dw_soj_imp_dtl_" + dealDate.replace("-", "");
    }
    
    public static String getUbaTableNameForIB(String dealDate)
    {
    	  return "dw_web_visit_traffic_log" + dealDate.replace("-", ""); 
    }   
    
}

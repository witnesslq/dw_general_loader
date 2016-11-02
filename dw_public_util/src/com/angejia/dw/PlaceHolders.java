package com.angejia.dw;

/**
 * 存储sql中可能出现的占位符（注意不是预编译的问号占位符），
 * 这些占位符需要在执行sql之前进行替换
 * @author larrysun
 *
 */
public class PlaceHolders
{
    /**
     * 数据导出到文件的目录
     */
    public static final String TMP_CATALOG = "${tempCatalog}";
    /**
     * 文件的后缀（与日期相关）
     */
    public static final String File_SUFFIX = "${outFileSuffix}";
    
    public static final String SOJ_TABLE = "${sojTable}";
    
    public static final String UBA_TABLE = "${ubaTable}";
    
    public static final String SOJ_TABLE_LAST_MONTH = "${sojTableLastMonth}";
    
    public static final String UBA_TABLE_LAST_MONTH = "${ubaTableLastMonth}";
    
    public static final String SOJ_TABLE_THIS_MONTH = "${sojTableThisMonth}";
    
    public static final String UBA_TABLE_THIS_MONTH = "${ubaTableThisMonth}";
    
    public static final String IW_TABLE = "${iwTable}";
    
    public static final String START_DAY = "${startDate}";
    
    public static final String END_DAY = "${endDate}";
    
    public static final String PropClicks_TABLE = "${propClicksTable}";
    
    //public static final String SERVER_CONFIG_PATH = "/home/dwadmin/dwetl/server_config/";
    
    public static final String MONTH_ID = "${monthId}";
    
    public static final String MONTH_BEGIN = "${monthBegin}";
    
    public static final String MONTH_END = "${monthEnd}";
    
    public static final String WEEK_BEGIN = "${weekBegin}";
    
    public static final String WEEK_END = "${weekEnd}";
    
    public static final String WEEK_ID = "${weekId}";
    
    public static final String DATE_SUFFIX = "${dateSuffix}";
    
    public static final String MONTH_SUFFIX = "${monthSuffix}";
    
    public static final String MONTH_ONLY_SUFFIX = "${monthOnlySuffix}";
    
    public static final String SEVEN_DAYS_BEFORE = "${sevenDaysBefore}";
    
}

package com.angejia.dw;

import org.springframework.jdbc.core.JdbcTemplate;

import com.angejia.dw.DWLogger;
import com.angejia.dw.PlaceHolders;
import com.angejia.dw.util.DWDateUtil;
import com.angejia.dw.util.SQLParseUtil;
import com.angejia.dw.util.TableNameUtil;

/**
 * 抽取程序使用的执行模板，为抽取sql提供文件名后缀替换，时间参数替换，执行，日志记录等功能
 * @author larrysun
 *
 */
public class ExtractRunner
{
    private JdbcTemplate jdbcTemplateRemote;
    private DWLogger logger;
    private String dbVendor;
    
    //导出文件的目录
    private String tempCatalog;
    
    /**
     * 程序开始日志记录
     * @param themeName 主题名
     * @param moduleName 模块名
     * @return 当前时间的毫秒数
     */
    public long begin(String themeName, String moduleName)
    {
        long time = System.currentTimeMillis();
        getLogger().log(time, themeName, moduleName, "", "Begin", "", "");
        return time;
    }
    
    /**
     * 程序结束日志记录
     * @param themeName 主题名
     * @param moduleName 模块名
     * @return 当前时间的毫秒数
     */
    public long end(String themeName,  String moduleName)
    {
        long time = System.currentTimeMillis();
        getLogger().log(time, themeName, moduleName, "", "End", "", "");
        return time;
    }
    
    /**
     * 执行抽取sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param startDate 抽取时间
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runExtractSql(String sqls, String startDate, String themeName, String moduleName)
    {
        runExtractSql(sqls, startDate, null, themeName, moduleName);
    }
    
    /**
     * 执行抽取sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param startDate 抽取开始时间
     * @param endDate 抽取结束时间，若此参数为null，退化为抽取某天
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runExtractSql(String sqls, String startDate, String endDate, String themeName, String moduleName)
    {
        String[] sqlArray = SQLParseUtil.removeComments(sqls).split(";");
        for(String sql : sqlArray)
        {
          //如果sql为空（意外情况）则跳过
            if(sql.trim().equals("")) 
                continue;
            
            long startTime = System.currentTimeMillis();
            
            //取得导出文件的名字，写日志时用
            String outFileName = SQLParseUtil.getOutFileName(sql);
            if(outFileName.length() > 50)
                outFileName = outFileName.substring(0,50);
            
            try
            {
                //替换导出文件的后缀名和文件目录
                //这里使用的replace方法，是不带正则的（因为没有必要），所以不用考虑转义
                sql = sql.replace(PlaceHolders.TMP_CATALOG, tempCatalog)
                         .replace(PlaceHolders.File_SUFFIX, endDate == null ? startDate : startDate + "_" + endDate)
                         .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(startDate));
                
                //替换开始时间和结束时间，如果运行某天也可以带结束时间，此时开始时间和结束时间相等
                sql = sql.replace(PlaceHolders.START_DAY, "'" + startDate + "'")
                         .replace(PlaceHolders.END_DAY, endDate == null ? ("'" + startDate + "'") : ("'" + endDate + "'"));
                
                sql = sql.replace(PlaceHolders.DATE_SUFFIX, startDate.replace("-", ""))
                         .replace(PlaceHolders.MONTH_SUFFIX, DWDateUtil.getMonthId(startDate, false));
                
                //替换soj（infobright专用情况）
                sql = sql.replace(PlaceHolders.SOJ_TABLE, TableNameUtil.getSojTableNameForIB(startDate));

                System.out.println(sql);
                
                if(dbVendor.equals("mysql"))
                jdbcTemplateRemote.setQueryTimeout(3600);
                
                jdbcTemplateRemote.execute(sql);
                               
                logger.log(startTime, themeName, moduleName, "write into file", "RunStop", outFileName, "");
            }
            catch (Exception e)
            {
                logger.log(startTime, themeName, moduleName, "write into file", "Exception", outFileName, e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 执行每小时抽取sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runExtractSqlHourly(String sqls, String themeName, String moduleName)
    {
        runExtractSql(sqls, "_hourly", null, themeName, moduleName);
    }
    
    public void setJdbcTemplateRemote(JdbcTemplate jdbcTemplateRemote)
    {
        this.jdbcTemplateRemote = jdbcTemplateRemote;
    }
    
    public JdbcTemplate getJdbcTemplateRemote()
    {
        return jdbcTemplateRemote;
    }

    public void setTempCatalog(String tempCatalog)
    {
        this.tempCatalog = tempCatalog;
    }
    
    public String getTempCatalog()
    {
        return tempCatalog;
    }

    public void setLogger(DWLogger logger)
    {
        this.logger = logger;
    }

    public DWLogger getLogger()
    {
        return logger;
    }

    public void setDbVendor(String dbVendor)
    {
        this.dbVendor = dbVendor;
    }

    public String getDbVendor()
    {
        return dbVendor;
    }
}

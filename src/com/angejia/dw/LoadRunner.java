package com.angejia.dw;

import org.springframework.jdbc.core.JdbcTemplate;

import com.angejia.dw.util.DWDateUtil;
import com.angejia.dw.util.SQLParseUtil;
import com.angejia.dw.DWLogger;
import com.angejia.dw.PlaceHolders;

/**
 * 导入程序使用的执行模板，为抽取sql提供文件名后缀替换，时间参数/占位符替换，执行，日志记录等功能
 * @author larrysun
 *
 */
public class LoadRunner
{
    private JdbcTemplate jdbcTemplate;
    private DWLogger logger;
    
    //导出文件的目录
    private String tempCatalog;
    
    private String extractPublic;
    private String extractAnjuke;
    private String extractAifang;
    private String extractHaozu;
    private String extractJinpu;
    private String extractAngejia;
    
    private String stagePublic;
    private String stageAnjuke;
    private String stageAifang;
    private String stageHaozu;
    private String stageJinpu;
    private String stageAngejia;
   
    private String dwPublic;
    private String dwAnjuke;
    private String dwAifang;
    private String dwHaozu;
    private String dwJinpu;
    private String dwAngejia;
    
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
    
    public void runLoadSql(String sqls, String themeName, String moduleName)
    {
        runLoadSql(sqls, "0000-00-00", null, themeName, moduleName, true);
    }
    
    /**
     * 执行导入sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param startDate 导入时间，与抽取时间相对应
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runLoadSql(String sqls, String startDate, String themeName, String moduleName)
    {
        runLoadSql(sqls, startDate, null, themeName, moduleName, true);
    }
    
    /**
     * 执行导入sql，支持一条以上的sql，但出现异常时只报警告
     * @param sqls 一条或多条sql
     * @param startDate 导入时间，与抽取时间相对应
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runLoadSqlWithWarning(String sqls, String startDate, String themeName, String moduleName)
    {
        runLoadSql(sqls, startDate, null, themeName, moduleName, false);
    }

    /**
     * 执行导入sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param startDate 导入开始时间，与抽取开始时间相对应
     * @param endDate 抽取结束时间，若此参数为null，退化为导入某天
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runLoadSql(String sqls, String startDate, String endDate, String themeName, String moduleName)
    {
        runLoadSql(sqls, startDate, endDate, themeName, moduleName, true);
    }
    
    /**
     * 执行每小时导入sql，支持一条以上的sql
     * @param sqls 一条或多条sql
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runLoadSqlHourly(String sqls, String themeName, String moduleName)
    {
        runLoadSql(sqls, "_hourly", null, themeName, moduleName);
    }
    
    /**
     * 执行导入sql，支持一条以上的sql，但出现异常时只报警告
     * @param sqls 一条或多条sql
     * @param startDate 导入开始时间，与抽取开始时间相对应
     * @param endDate 抽取结束时间，若此参数为null，退化为导入某天
     * @param themeName 程序的主题名
     * @param moduleName 这组sql程序的模块名
     */
    public void runLoadSqlWithWarning(String sqls, String startDate, String endDate, String themeName, String moduleName)
    {
        runLoadSql(sqls, startDate, endDate, themeName, moduleName, false);
    }
    
    private void runLoadSql(String sqls, String startDate, String endDate, String themeName, String moduleName, boolean exception)
    {
        String[] sqlArray = SQLParseUtil.removeComments(sqls).split(";");
        for (String sql : sqlArray)
        {
            // 如果sql为空（意外情况）则跳过
            if (sql.trim().equals(""))
                continue;

            long startTime = System.currentTimeMillis();
            String sqlAction = SQLParseUtil.getSqlAction(sql);
            String tableName = SQLParseUtil.getTableName(sql);
            
            try
            {
                //替换导出文件的后缀名和文件目录
                //这里使用的replace方法，是不带正则的（因为没有必要），所以不用考虑转义
                sql = sql.replace(PlaceHolders.TMP_CATALOG, tempCatalog)
                         .replace(PlaceHolders.File_SUFFIX, endDate == null ? startDate : startDate + "_" + endDate);
                
                //替换开始时间和结束时间，如果运行某天也可以带结束时间，此时开始时间和结束时间相等
                sql = sql.replace(PlaceHolders.START_DAY, "'" + startDate + "'")
                         .replace(PlaceHolders.END_DAY, endDate == null ? ("'" + startDate + "'") : ("'" + endDate + "'"))
                         .replace(PlaceHolders.MONTH_ID, "'" + DWDateUtil.getMonthId(startDate, true) + "'")
            			 .replace(PlaceHolders.MONTH_BEGIN, "'" + DWDateUtil.getMonthBegin(startDate) + "'")
            			 .replace(PlaceHolders.MONTH_END, "'" + DWDateUtil.getMonthEnd(startDate) + "'")
            			 .replace(PlaceHolders.WEEK_ID, "'" + DWDateUtil.getWeekIdEn(startDate) + "'")
            			 .replace(PlaceHolders.WEEK_BEGIN, "'" + DWDateUtil.getWeekBeginDtEn(startDate) + "'")
            			 .replace(PlaceHolders.WEEK_END, "'" + DWDateUtil.getWeekEndDtEn(startDate) + "'")
            			 .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(startDate))
            			 .replace(PlaceHolders.SEVEN_DAYS_BEFORE, DWDateUtil.addDays(startDate, -7));
                
                //替换库名
                sql = sql.replace("${dw_extract_public}", this.extractPublic)
                           .replace("${dw_extract_anjuke}", this.extractAnjuke)
                           .replace("${dw_extract_aifang}", this.extractAifang)
                           .replace("${dw_extract_haozu}", this.extractHaozu)
                           .replace("${dw_extract_jinpu}", this.extractJinpu)
                           .replace("${dw_extract_angejia}", this.extractAngejia);
                
                sql = sql.replace("${dw_stage_public}", this.stagePublic)
                           .replace("${dw_stage_anjuke}", this.stageAnjuke)
                           .replace("${dw_stage_aifang}", this.stageAifang)
                           .replace("${dw_stage_haozu}", this.stageHaozu)
                           .replace("${dw_stage_jinpu}", this.stageJinpu)
                           .replace("${dw_stage_angejia}", this.stageAngejia);
                
                sql = sql.replace("${dw_db_public}", this.dwPublic)
                           .replace("${dw_db_anjuke}", this.dwAnjuke)
                           .replace("${dw_db_aifang}", this.dwAifang)
                           .replace("${dw_db_haozu}", this.dwHaozu)
                           .replace("${dw_db_jinpu}", this.dwJinpu)
                           .replace("${dw_db_angejia}", this.dwAngejia);
                
                System.out.println(sql);
                
                long result = 0;
                //准备时间戳占位符的替换
                int qNum = SQLParseUtil.getQuestionMarkNum(sql);
                if (qNum > 0)
                {
                    //含有时间戳占位符
                    String[] qParams = new String[qNum];
                    if(endDate == null)
                    {
                        //只有一个时间参数
                        for(int i = 0; i < qNum; i++)
                          qParams[i] = startDate;
                    }
                    else
                    {
                        //这里假设start date和end date一定是成对出现的,
                        //而且start date出现在end date之前
                        for(int i = 0; i < qNum; i += 2)
                        {
                            qParams[i] = startDate;
                            qParams[i + 1] = endDate;
                        }     
                    }

                    result = jdbcTemplate.update(sql, qParams);
                }
                else
                {
                    result = jdbcTemplate.update(sql);
                }
                
                
                logger.log(startTime, themeName, moduleName, sqlAction, "RunStop", tableName, String.valueOf(result));
            }
            catch (Exception e)
            {
                logger.log(startTime, themeName, moduleName, sqlAction, exception ? "Exception" : "Warning", tableName, e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getJdbcTemplate()
    {
        return jdbcTemplate;
    }

    public void setLogger(DWLogger logger)
    {
        this.logger = logger;
    }

    public DWLogger getLogger()
    {
        return logger;
    }

    public void setTempCatalog(String tempCatalog)
    {
        this.tempCatalog = tempCatalog;
    }

    public String getTempCatalog()
    {
        return tempCatalog;
    }

    public void setExtractPublic(String extractPublic)
    {
        this.extractPublic = extractPublic;
    }

    public String getExtractPublic()
    {
        return extractPublic;
    }

    public void setExtractAnjuke(String extractAnjuke)
    {
        this.extractAnjuke = extractAnjuke;
    }

    public String getExtractAnjuke()
    {
        return extractAnjuke;
    }

    public void setExtractAifang(String extractAifang)
    {
        this.extractAifang = extractAifang;
    }

    public String getExtractAifang()
    {
        return extractAifang;
    }

    public void setExtractHaozu(String extractHaozu)
    {
        this.extractHaozu = extractHaozu;
    }

    public String getExtractHaozu()
    {
        return extractHaozu;
    }

    public void setExtractJinpu(String extractJinpu)
    {
        this.extractJinpu = extractJinpu;
    }

    public String getExtractJinpu()
    {
        return extractJinpu;
    }

    public void setStagePublic(String stagePublic)
    {
        this.stagePublic = stagePublic;
    }

    public String getStagePublic()
    {
        return stagePublic;
    }

    public void setStageAnjuke(String stageAnjuke)
    {
        this.stageAnjuke = stageAnjuke;
    }

    public String getStageAnjuke()
    {
        return stageAnjuke;
    }

    public void setStageAifang(String stageAifang)
    {
        this.stageAifang = stageAifang;
    }

    public String getStageAifang()
    {
        return stageAifang;
    }

    public void setStageHaozu(String stageHaozu)
    {
        this.stageHaozu = stageHaozu;
    }

    public String getStageHaozu()
    {
        return stageHaozu;
    }

    public void setStageJinpu(String stageJinpu)
    {
        this.stageJinpu = stageJinpu;
    }

    public String getStageJinpu()
    {
        return stageJinpu;
    }

    public void setDwPublic(String dwPublic)
    {
        this.dwPublic = dwPublic;
    }

    public String getDwPublic()
    {
        return dwPublic;
    }

    public void setDwAnjuke(String dwAnjuke)
    {
        this.dwAnjuke = dwAnjuke;
    }

    public String getDwAnjuke()
    {
        return dwAnjuke;
    }

    public void setDwAifang(String dwAifang)
    {
        this.dwAifang = dwAifang;
    }

    public String getDwAifang()
    {
        return dwAifang;
    }

    public void setDwHaozu(String dwHaozu)
    {
        this.dwHaozu = dwHaozu;
    }

    public String getDwHaozu()
    {
        return dwHaozu;
    }

    public void setDwJinpu(String dwJinpu)
    {
        this.dwJinpu = dwJinpu;
    }

    public String getDwJinpu()
    {
        return dwJinpu;
    }
    
    public void setExtractAngejia(String extractAngejia)
    {
        this.extractAngejia = extractAngejia;
    }

    public String getExtractAngejia()
    {
        return extractAngejia;
    }
    
    public void setStageAngejia(String stageAngejia)
    {
        this.stageAngejia = stageAngejia;
    }

    public String getStageAngejia()
    {
        return stageAngejia;
    }
    
    public void setDwAngejia(String dwAngejia)
    {
        this.dwAngejia = dwAngejia;
    }

    public String getDwAngejia()
    {
        return dwAngejia;
    }

}

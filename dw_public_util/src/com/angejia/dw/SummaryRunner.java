package com.angejia.dw;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.angejia.dw.util.DWDateUtil;
import com.angejia.dw.util.SQLParseUtil;
import com.angejia.dw.util.TableNameUtil;
import com.angejia.dw.DWLogger;
import com.angejia.dw.PlaceHolders;

/**
 * 汇总程序使用的执行模板，为汇总sql提供表名替换，时间参数替换，执行，日志记录等功能
 * @author larrysun
 *
 */
public class SummaryRunner
{
    private JdbcTemplate jdbcTemplate;
    private DWLogger logger;
    private Boolean dropTableFinally;
    private String dbVendor;

    private String extractPublic;
//    private String extractAnjuke;
//    private String extractAifang;
//    private String extractHaozu;
//    private String extractJinpu;
    private String extractAngejia;

    private String stagePublic;
//    private String stageAnjuke;
//    private String stageAifang;
//    private String stageHaozu;
//    private String stageJinpu;
    private String stageAngejia;

    private String dwPublic;
//    private String dwAnjuke;
//    private String dwAifang;
//    private String dwHaozu;
//    private String dwJinpu;
    private String dwAngejia;

    private String dwSnap;

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
     * 如果这个程序这天不产生数据（没有执行任何sql），
     * 那么调用这个方法告诉监控程序正常运行
     * @param themeName
     * @param moduleName
     */
    public void logHeartBeat(String themeName,  String moduleName)
    {
        logger.log(System.currentTimeMillis(), themeName, moduleName,
                "heartbeat", "RunStop", "heartbeat", "");
    }

    /**
     * 执行一组sql以完成一个或多个汇总表
     * @param dropSqls 删除临时表的sql
     * @param mainSqls 创建临时表以及更新汇总表的sql
     * @param startDate 汇总开始日期
     * @param themeName 程序主题名
     * @param moduleName 该组sql的模块名
     */
    public void runSummarySql(String dropSqls, String mainSqls, String startDate, String themeName, String moduleName)
    {
        boolean hasDropSql = (dropSqls != null && !dropSqls.trim().equals(""));
        if(hasDropSql)
            this.runDropSqlInBatch(dropSqls, themeName, moduleName);

        runSummarySql(mainSqls, startDate, themeName, moduleName);

        //有时需要留下临时表供调试，通过dropTableFinally配置
        if(dropTableFinally && hasDropSql)
            this.runDropSqlInBatch(dropSqls, themeName, moduleName);
    }

    /**
     * 执行一组sql以完成一个或多个汇总表
     * @param mainSqls 创建临时表以及更新汇总表的sql
     * @param startDate 汇总开始日期
     * @param themeName 程序主题名
     * @param moduleName 该组sql的模块名
     */
    public void runSummarySql(String mainSqls, String startDate, String themeName, String moduleName)
    {
        long startTime = System.currentTimeMillis();
        String[] sqlArray = SQLParseUtil.removeComments(mainSqls).split(";");
        for (String sql : sqlArray)
        {
            // 如果sql为空（意外情况）则跳过
            if (sql.trim().equals(""))
                continue;

            startTime = System.currentTimeMillis();
            String sqlAction = SQLParseUtil.getSqlAction(sql);
            String tableName = SQLParseUtil.getTableName(sql);

            try
            {
                //替换时间和soj表
                sql = sql.replace(PlaceHolders.START_DAY, "'" + startDate + "'")
                         .replace(PlaceHolders.MONTH_ID, "'" + DWDateUtil.getMonthId(startDate, true) + "'")
                         .replace(PlaceHolders.MONTH_BEGIN, "'" + DWDateUtil.getMonthBegin(startDate) + "'")
                         .replace(PlaceHolders.MONTH_END, "'" + DWDateUtil.getMonthEnd(startDate) + "'")
                         .replace(PlaceHolders.WEEK_ID, "'" + DWDateUtil.getWeekIdEn(startDate) + "'")
                         .replace(PlaceHolders.WEEK_BEGIN, "'" + DWDateUtil.getWeekBeginDtEn(startDate) + "'")
                         .replace(PlaceHolders.WEEK_END, "'" + DWDateUtil.getWeekEndDtEn(startDate) + "'")
                         .replace(PlaceHolders.MONTH_SUFFIX, DWDateUtil.getMonthId(startDate, false))
                         .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(startDate))
                         .replace(PlaceHolders.SEVEN_DAYS_BEFORE, DWDateUtil.addDays(startDate, -7));

                sql = sql.replace(PlaceHolders.SOJ_TABLE, TableNameUtil.getSojTableName(startDate));
                sql = sql.replace(PlaceHolders.SOJ_TABLE_LAST_MONTH,
                        TableNameUtil.getSojMonthTableName(DWDateUtil.addMonths(startDate, -1)));
                sql = sql.replace(PlaceHolders.SOJ_TABLE_THIS_MONTH, TableNameUtil.getSojMonthTableName(startDate));
                sql = sql.replace(PlaceHolders.IW_TABLE, TableNameUtil.getInfoWindowTableName(startDate));
                sql = sql.replace(PlaceHolders.PropClicks_TABLE, TableNameUtil.getPropClickTableName(startDate));

                //替换库名
                sql = sql.replace("${dw_extract_public}", this.extractPublic)
//                           .replace("${dw_extract_anjuke}", this.extractAnjuke)
//                           .replace("${dw_extract_aifang}", this.extractAifang)
//                           .replace("${dw_extract_haozu}", this.extractHaozu)
//                           .replace("${dw_extract_jinpu}", this.extractJinpu)
                           .replace("${dw_extract_angejia}", this.extractAngejia);

                sql = sql.replace("${dw_stage_public}", this.stagePublic)
//                           .replace("${dw_stage_anjuke}", this.stageAnjuke)
//                           .replace("${dw_stage_aifang}", this.stageAifang)
//                           .replace("${dw_stage_haozu}", this.stageHaozu)
//                           .replace("${dw_stage_jinpu}", this.stageJinpu)
                           .replace("${dw_stage_angejia}", this.stageAngejia);

                sql = sql.replace("${dw_db_public}", this.dwPublic)
//                           .replace("${dw_db_anjuke}", this.dwAnjuke)
//                           .replace("${dw_db_aifang}", this.dwAifang)
//                           .replace("${dw_db_haozu}", this.dwHaozu)
//                           .replace("${dw_db_jinpu}", this.dwJinpu)
                           .replace("${dw_db_angejia}", this.dwAngejia)
                           .replace("${dw_snap}", this.dwSnap);

                System.out.println(sql);

                long result = 0;
                //准备时间戳占位符的替换
                int qNum = SQLParseUtil.getQuestionMarkNum(sql);
                if (qNum > 0)
                {
                    //含有时间戳占位符
                    String[] qParams = new String[qNum];
                    for(int i = 0; i < qNum; i++)
                      qParams[i] = startDate;

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
                logger.log(startTime, themeName, moduleName, sqlAction, "Exception", tableName, e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void runSummarySql(String mainSqls, String themeName, String moduleName)
    {
        runSummarySql(mainSqls, "0000-00-00", themeName, moduleName);
    }

    /**
     * 执行一组sql以完成一个或多个汇总表
     * @param dropSqls 删除临时表的sql
     * @param mainSqls 创建临时表以及更新汇总表的sql
     * @param startDate 汇总开始日期
     * @param endDate 汇总结束日期，若此参数为空，则退化为做单日汇总
     * @param themeName 程序主题名
     * @param moduleName 该组sql的模块名
     */
    public void runSummarySql(String dropSqls, String mainSqls,
            String startDate, String endDate, String themeName, String moduleName)
    {
        if(endDate == null)
            this.runSummarySql(dropSqls, mainSqls, startDate, themeName, moduleName);

        else
        {
            Date startDay = DWDateUtil.parseDate(startDate);
            Date endDay = DWDateUtil.parseDate(endDate);

            for (Date i = startDay; i.compareTo(endDay) <= 0; i = DateUtils.addDays(i, 1))
            {
                String dealDate = DWDateUtil.formatDate(i);
                runSummarySql(dropSqls, mainSqls, dealDate, themeName, moduleName);
            }
        }
    }

    /**
     * drop大量临时表
     * @param sqls drop操作sql
     * @param themeName 程序主题名
     * @param moduleName 程序模块名
     */
    public void runDropSqlInBatch(String sqls, String themeName, String moduleName)
    {

        List<String> sqlList = new ArrayList<String>();
        for (String sql : SQLParseUtil.removeComments(sqls).split(";")) {
            if (sql.trim().isEmpty()) {
                continue;
            }
            sql = replaceDBName(sql);
            System.out.println(sql);
            sqlList.add(sql);
        }

        long start_time=System.currentTimeMillis();
        try{
            jdbcTemplate.batchUpdate(sqlList.toArray(new String[0]));
            logger.log(start_time, themeName, moduleName, "drop", "RunStop", "temp tables", "");
        } catch(Exception e) {
            logger.log(start_time, themeName, moduleName, "drop", "Exception", "temp tables", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 专门用来转义数据到历史表的sql组合
     * @param insertHisSql
     * @param removeSql
     * @param dealDate
     * @param themeName
     * @param moduleName
     */
    public void runMoveToHisSql(String insertHisSql, String removeSql, String dealDate, String themeName, String moduleName)
    {
        long start_time=System.currentTimeMillis();

        insertHisSql = replaceDBName(insertHisSql);
        removeSql = replaceDBName(removeSql);

        System.out.println(insertHisSql);
        System.out.println(removeSql);

        String tableNameHis = SQLParseUtil.getTableName(insertHisSql);
        String tableName = SQLParseUtil.getTableName(removeSql);

        try
        {
            long result = jdbcTemplate.update(insertHisSql);
            logger.log(start_time, themeName, moduleName, "insert", "RunStop", tableNameHis, String.valueOf(result));

            //若插入失败则不执行删除，以免丢失数据
            start_time = System.currentTimeMillis();
            try
            {
                result = jdbcTemplate.update(removeSql);
                logger.log(start_time, themeName, moduleName, "delete", "RunStop", tableName, String.valueOf(result));
            }
            catch (Exception e)
            {
                logger.log(start_time, themeName, moduleName, "delete", "Exception", tableName, e.getMessage());
                e.printStackTrace();
            }
        }
        catch (Exception e)
        {
            logger.log(start_time, themeName, moduleName, "insert", "Exception", tableNameHis, e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 查询数据
     * @param sql
     * @param dealDate
     * @param themeName
     * @param moduleName
     * @return
     */
    public SqlRowSet queryForRowSet(String sql, String dealDate, String themeName, String moduleName)
    {
        System.out.println(sql);
        SqlRowSet rowSet = null;
        long start_time = System.currentTimeMillis();
        String tableName = SQLParseUtil.getTableName(sql);
        try
        {
            // 准备时间戳占位符的替换
            int qNum = SQLParseUtil.getQuestionMarkNum(sql);
            if (qNum > 0)
            {
                // 含有时间戳占位符
                String[] qParams = new String[qNum];
                for (int i = 0; i < qNum; i++)
                    qParams[i] = dealDate;

                rowSet = jdbcTemplate.queryForRowSet(sql, qParams);
            }
            else
            {
                rowSet = jdbcTemplate.queryForRowSet(sql);
            }

            logger.log(start_time, themeName, moduleName, "query", "RunStop", tableName, "");
        }
        catch (Exception e)
        {
            logger.log(start_time, themeName, moduleName, "query", "Exception", tableName, e.getMessage());
        }
        return rowSet;
    }

    private String replaceDBName(String sql)
    {
        sql = sql.replace("${dw_extract_public}", this.extractPublic)
//        .replace("${dw_extract_anjuke}", this.extractAnjuke)
//        .replace("${dw_extract_aifang}", this.extractAifang)
//        .replace("${dw_extract_haozu}", this.extractHaozu)
//        .replace("${dw_extract_jinpu}", this.extractJinpu)
        .replace("${dw_extract_angejia}", this.extractAngejia);

        sql = sql.replace("${dw_stage_public}", this.stagePublic)
//        .replace("${dw_stage_anjuke}", this.stageAnjuke)
//        .replace("${dw_stage_aifang}", this.stageAifang)
//        .replace("${dw_stage_haozu}", this.stageHaozu)
//        .replace("${dw_stage_jinpu}", this.stageJinpu)
        .replace("${dw_stage_angejia}", this.stageAngejia);

        sql = sql.replace("${dw_db_public}", this.dwPublic)
//        .replace("${dw_db_anjuke}", this.dwAnjuke)
//        .replace("${dw_db_aifang}", this.dwAifang)
//        .replace("${dw_db_haozu}", this.dwHaozu)
//        .replace("${dw_db_jinpu}", this.dwJinpu)
        .replace("${dw_db_angejia}", this.dwAngejia)
        .replace("${dw_snap}", this.dwSnap);

        return sql;
    }

    /**
     * 查询数据
     * @param sql
     * @param themeName
     * @param moduleName
     * @return
     */
    public SqlRowSet queryForRowSet(String sql, String themeName, String moduleName)
    {
       return queryForRowSet(sql, "0000-00-00", themeName, moduleName);
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

    public void setDropTableFinally(Boolean dropTableFinally)
    {
        this.dropTableFinally = dropTableFinally;
    }

    public Boolean getDropTableFinally()
    {
        return dropTableFinally;
    }

    public void setExtractPublic(String extractPublic)
    {
        this.extractPublic = extractPublic;
    }

    public String getExtractPublic()
    {
        return extractPublic;
    }

//    public void setExtractAnjuke(String extractAnjuke)
//    {
//        this.extractAnjuke = extractAnjuke;
//    }
//
//    public String getExtractAnjuke()
//    {
//        return extractAnjuke;
//    }
//
//    public void setExtractAifang(String extractAifang)
//    {
//        this.extractAifang = extractAifang;
//    }
//
//    public String getExtractAifang()
//    {
//        return extractAifang;
//    }
//
//    public void setExtractHaozu(String extractHaozu)
//    {
//        this.extractHaozu = extractHaozu;
//    }
//
//    public String getExtractHaozu()
//    {
//        return extractHaozu;
//    }
//
//    public void setExtractJinpu(String extractJinpu)
//    {
//        this.extractJinpu = extractJinpu;
//    }
//
//    public String getExtractJinpu()
//    {
//        return extractJinpu;
//    }

    public void setStagePublic(String stagePublic)
    {
        this.stagePublic = stagePublic;
    }

    public String getStagePublic()
    {
        return stagePublic;
    }

//    public void setStageAnjuke(String stageAnjuke)
//    {
//        this.stageAnjuke = stageAnjuke;
//    }
//
//    public String getStageAnjuke()
//    {
//        return stageAnjuke;
//    }
//
//    public void setStageAifang(String stageAifang)
//    {
//        this.stageAifang = stageAifang;
//    }
//
//    public String getStageAifang()
//    {
//        return stageAifang;
//    }
//
//    public void setStageHaozu(String stageHaozu)
//    {
//        this.stageHaozu = stageHaozu;
//    }
//
//    public String getStageHaozu()
//    {
//        return stageHaozu;
//    }
//
//    public void setStageJinpu(String stageJinpu)
//    {
//        this.stageJinpu = stageJinpu;
//    }
//
//    public String getStageJinpu()
//    {
//        return stageJinpu;
//    }

    public void setDwPublic(String dwPublic)
    {
        this.dwPublic = dwPublic;
    }

    public String getDwPublic()
    {
        return dwPublic;
    }

//    public void setDwAnjuke(String dwAnjuke)
//    {
//        this.dwAnjuke = dwAnjuke;
//    }
//
//    public String getDwAnjuke()
//    {
//        return dwAnjuke;
//    }
//
//    public void setDwAifang(String dwAifang)
//    {
//        this.dwAifang = dwAifang;
//    }
//
//    public String getDwAifang()
//    {
//        return dwAifang;
//    }
//
//    public void setDwHaozu(String dwHaozu)
//    {
//        this.dwHaozu = dwHaozu;
//    }
//
//    public String getDwHaozu()
//    {
//        return dwHaozu;
//    }
//
//    public void setDwJinpu(String dwJinpu)
//    {
//        this.dwJinpu = dwJinpu;
//    }
//
//    public String getDwJinpu()
//    {
//        return dwJinpu;
//    }

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

    public void setDwSnap(String dwSnap)
    {
        this.dwSnap = dwSnap;
    }

    public String getDwSnap()
    {
        return dwSnap;
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

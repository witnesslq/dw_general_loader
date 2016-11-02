package com.angejia.dw;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 日志记录工具
 * @author larrysun
 *
 */
public class DWLogger
{
    private JdbcTemplate jdbcTemplate;
    public static boolean hasException = false;
    public static boolean logException = false;
    public static List<String> exceptions = new ArrayList<String>();

    /**
     * 记录每一条/一批sql的执行结果
     * @param startTime sql执行的开始时间
     * @param themeName 这个程序的名字
     * @param moduleName 程序中一组相关sql的名字
     * @param action sql的行为（如insert，load，truncate）
     * @param step 这步是否成功
     * @param fileName 操作的表名或者文件名
     * @param result 详细的结果（执行成功就是记录数，失败就是失败原因）
     */
    public void log(long startTime, String themeName, String moduleName,
                    String action, String step, String fileName, String result)
    {
        long endTime = System.currentTimeMillis();
        float costTime = (endTime - startTime) / 1000;
        String sql = " insert into dw_monitor.dw_etl_log(start_time,end_time,cost_time,theme_name,module_name,action,step,fileOrTable,result)" + " values (?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[] { new Timestamp(startTime), new Timestamp(endTime), costTime,
                                         themeName, moduleName, action, step, fileName, result };
        try
        {
            jdbcTemplate.update(sql, params);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        if ("Exception".equals(step)) {
            hasException = true;
            if (logException) {
                exceptions.add(result);
            }
        }
    }
    
    //injects
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getJdbcTemplate()
    {
        return jdbcTemplate;
    }
}

package com.angejia.dw.util;


import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.angejia.dw.SummaryRunner;

public class CaseWhenConditonUtil {
	/**
	 * 获取配置在表里面的case when 条件
     * 参照表dw_marketing_get_type_config建case when 的条件配置
	 * @param runner
	 * @param conifgTable
	 */
    public static String getWhenSql(SummaryRunner runner,String conifgTable)
    {
    	if(!conifgTable.contains(".")){
    		//默认数据库为dw_db
    		conifgTable = "dw_db."+conifgTable;
    	}	
    	String getConiditonSQL = "select `sql`,result from "+conifgTable+" where isvalid=1 order by rank asc";
	    SqlRowSet whenConditionList = runner.getJdbcTemplate().queryForRowSet(getConiditonSQL);
	    StringBuffer whenSql =new StringBuffer();
        while (whenConditionList.next())
        	whenSql.append(" when "+whenConditionList.getString("sql")+" then "+whenConditionList.getString("result")+" \n");
        
        return whenSql.toString();
    }  
}

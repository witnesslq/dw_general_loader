package com.angejia.dw.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.angejia.dw.PlaceHolders;

/**
 * 解析sql获得特定信息。基本属于内部使用
 * @author larrysun
 *
 */
public class SQLParseUtil
{
    /**
     * 返回一个预编译sql中问号占位符的个数。
     * @param sql
     * @return 问号占位符的个数
     */
    public static int getQuestionMarkNum(String sql)
    {
        int count = 0;
        //如果?在字符串内（如varchar）则无效，此标志位判断当前字符是否在字符串内
        boolean inQuote = false;
        for(int i = 0; i < sql.length(); i++)
        {
            if(sql.charAt(i) == '\'')
                inQuote = !inQuote;
                
            if(sql.charAt(i) == '?' && !inQuote)
                count++;
        }
        return count;
    }
    
    /**
     * 返回导出数据到文件（select...into outfile）的文件名
     * @param sql
     * @return 文件名
     */
    public static String getOutFileName(String sql)
    {
        try
        {
            int tempCatalogIdx = sql.indexOf(PlaceHolders.TMP_CATALOG);
            int tempCatalogStrLen = PlaceHolders.TMP_CATALOG.length();
            //找到开始点
            int outFileNameIdx = tempCatalogIdx + tempCatalogStrLen;
            //找到结束点
            int outFileSuffixIdx = sql.indexOf(PlaceHolders.File_SUFFIX);
            if(outFileSuffixIdx < 0)
                outFileSuffixIdx = sql.length() - 1;
            return sql.substring(outFileNameIdx, outFileSuffixIdx);
        }
        catch (StringIndexOutOfBoundsException e)
        {
            //万一解析有问题也不影响执行
            return "can not get file name, please check.";
        }
    }
    
    /**
     * 返回sql执行的动作(如insert，create table等)
     * @param sql
     * @return sql执行的动作
     */
    public static String getSqlAction(String sql)
    {
        sql = removeCommentOnTop(sql).toLowerCase().trim();
        String[] actions = {"alter table", "create table ", "create index ", "drop table ", "drop index ",
                "rename table ", "load ", "insert ", "delete ", "truncate ", "update ", "select ", "replace ","optimize table","lzoindexer"} ;
        
        for(String action : actions)
            if(sql.toLowerCase().indexOf(action) == 0)
                return action;

        return null;
    }
    
    /**
     * 返回sql定义/操作的表（不带库名）
     * @param sql
     * @return 表名
     */
    public static String getTableName(String sql)
    {
        sql = removeCommentOnTop(sql).toLowerCase().trim();
        
        Map<String, String[]> parse = new HashMap<String, String[]>();
        parse.put("alter table ", new String[]{" add", " drop", " modify", " alter", " change", " rename"});
        parse.put("create table ", new String[]{"as select", "like", "("});
        parse.put(" on ", new String[]{"("}); //create index
        parse.put("drop table if exists ", new String[]{});
        parse.put("rename table ", new String[]{" to"});
        parse.put("truncate table ", new String[]{});
        parse.put("optimize table ", new String[]{});
        parse.put(" into table ", new String[]{" character set", " fields terminated by"}); //load
        parse.put("insert ", new String[]{"(", " select"});
        parse.put("insert into ", new String[]{"(", " select"});
        parse.put("insert ignore into ", new String[]{"(", " select"});
        parse.put("insert overwrite table ", new String[]{" partition", " select"}); // hive插入专用语法
        parse.put("replace ", new String[]{"(", " select"});
        parse.put("replace into ", new String[]{"(", " select"});
        parse.put("update ", new String[]{" set", " left join", " join"});//注意表的别名
        parse.put("delete from ", new String[]{" where"});
        parse.put("delete ", new String[]{","});//多表关联delete,注意表的别名
        parse.put(" from", new String[]{"where", "group by", "order by"}); //select(放在最后判断)
        
        for(Iterator<Entry<String, String[]>>  it = parse.entrySet().iterator(); it.hasNext();)
        {
            Entry<String, String[]> entry = it.next();
            String start = entry.getKey();
            String[] end = entry.getValue();
            //是否有起始点
            if(sql.indexOf(start) == 0 || (sql.indexOf(start) > 0 && (sql.indexOf("create index") == 0 
                    || sql.indexOf("load") == 0 || sql.indexOf("select") == 0)))
            {
                int tableNameStartIdx = sql.indexOf(start) + start.length();
                int tableNameEndIdx = -1;
                
                if(end.length == 0) // 没有后文的情况（drop table,truncate,optimize）
                    tableNameEndIdx = sql.length();
                else
                    for(String s : end)
                        if(sql.indexOf(s) > -1 && tableNameEndIdx == -1)
                            tableNameEndIdx = sql.indexOf(s);
                
                if(tableNameEndIdx > 0) //找到匹配
                {
                    try
                    {
                        String tableLong = sql.substring(tableNameStartIdx, tableNameEndIdx).trim();

                        //去除库名、别名
                        int dotIdx = tableLong.indexOf(".");
                        int spaceIdx = tableLong.indexOf(" ");
                        if(spaceIdx > -1)
                            return tableLong.substring(dotIdx + 1, spaceIdx);
                        else
                            return tableLong.substring(dotIdx + 1);
                    }
                    catch (StringIndexOutOfBoundsException e)
                    {
                        // 万一解析有问题也不影响执行
                        return "can not get table, please check.";
                    }
                }    
            }
        }
        
        //截取sql前面80个字符供分析
        return sql.substring(0, sql.length() > 80 ? 80 : sql.length());
    }
    
    /**
     * 去除sql字符串开头的注释，为了后面解析sql的动作和操作的表
     * @param sql
     * @return
     */
    private static String removeCommentOnTop(String sql)
    {
        sql = sql.trim();
        int startPos = sql.indexOf("/*");
        if(startPos != 0) return sql;
        //递归调用防止多个注释
        else return removeCommentOnTop(sql.substring(sql.indexOf("*/") + 2));
    }

    public static String removeComments(String sql) {
        sql = sql.replaceAll("(?s)/\\*.*?\\*/", "");
        return sql;
    }

}

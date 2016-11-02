package com.angejia.dw.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.angejia.dw.util.SQLReaderUtil;

/**
 * 读取sql文件的方法
 * @author larrysun
 *
 */
public class SQLReaderUtil
{
    /**
     * 读取jar包内的sql文件
     * @param filePath sql在jar包内的路径
     * @throws IOException
     * @return sql代码
     */
    public static String readSQLFile(String filePath) throws IOException
    {
        InputStream is = SQLReaderUtil.class.getResourceAsStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null)
            sb.append(line + " ");
        
        return sb.toString();
    }
    
    public static String readSQLFileExternal(String filePath) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null)
            sb.append(line + " ");
        
        return sb.toString();
    }
}

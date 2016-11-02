package com.angejia.dw.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class DBConfigUtil
{
    public static String getDBName(String sql) throws FileNotFoundException, IOException
    {
        Properties properties = new Properties();
        properties.load (new FileInputStream ("db_name.properties"));

        String dwPublic = properties.getProperty("dw.public");
//        String dwAnjuke = properties.getProperty("dw.anjuke");
//        String dwAifang = properties.getProperty("dw.aifang");
//        String dwHaozu = properties.getProperty("dw.haozu");
//        String dwJinpu = properties.getProperty("dw.jinpu");
        String dwAngejia = properties.getProperty("dw.angejia");

        String stagePublic = properties.getProperty("stage.public");
//        String stageAnjuke = properties.getProperty("stage.anjuke");
//        String stageAifang = properties.getProperty("stage.aifang");
//        String stageHaozu = properties.getProperty("stage.haozu");
//        String stageJinpu = properties.getProperty("stage.jinpu");
        String stageAngejia = properties.getProperty("stage.angejia");

        String extractPublic = properties.getProperty("extract.public");
//        String extractAnjuke = properties.getProperty("extract.anjuke");
//        String extractAifang = properties.getProperty("extract.aifang");
//        String extractHaozu = properties.getProperty("extract.haozu");
//        String extractJinpu = properties.getProperty("extract.jinpu");
        String extractAngejia = properties.getProperty("extract.angejia");

        String dwSnap = properties.getProperty("dw.snap");

        sql = sql.replace("${dw_extract_public}", extractPublic)
//        .replace("${dw_extract_anjuke}", extractAnjuke)
//        .replace("${dw_extract_aifang}", extractAifang)
//        .replace("${dw_extract_haozu}", extractHaozu)
//        .replace("${dw_extract_jinpu}", extractJinpu)
        .replace("${dw_extract_angejia}", extractAngejia);

        sql = sql.replace("${dw_stage_public}", stagePublic)
//        .replace("${dw_stage_anjuke}", stageAnjuke)
//        .replace("${dw_stage_aifang}", stageAifang)
//        .replace("${dw_stage_haozu}", stageHaozu)
//        .replace("${dw_stage_jinpu}", stageJinpu)
        .replace("${dw_stage_angejia}", stageAngejia);

        sql = sql.replace("${dw_db_public}", dwPublic)
//        .replace("${dw_db_anjuke}", dwAnjuke)
//        .replace("${dw_db_aifang}", dwAifang)
//        .replace("${dw_db_haozu}", dwHaozu)
//        .replace("${dw_db_jinpu}", dwJinpu)
        .replace("${dw_db_angejia}", dwAngejia);

        sql = sql.replace("${dw_snap}", dwSnap);

        return sql;
    }
}

package com.angejia.dw.hive;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.angejia.dw.DWLogger;
import com.angejia.dw.ExtractRunner;
import com.angejia.dw.PlaceHolders;
import com.angejia.dw.SummaryRunner;
import com.angejia.dw.util.DWDateUtil;
import com.angejia.dw.util.SQLParseUtil;
import com.angejia.dw.util.TeeOutputStream;
import com.angejia.dw.util.UnclosableOutputStream;

public class HiveRunner
{
    private SummaryRunner summaryRunner;
    private ExtractRunner extractRunner;
    private String tempCatalog;
    private boolean testMode = false;
    private Pattern ptrnLzoIndexer = Pattern.compile("(?i)LZOINDEXER\\s+(.+)");

    public  String tmpFolderForHDFS;
    public String getTmpFolderForHDFS() {
        return this.tmpFolderForHDFS;
    }
    public void setTmpFolderForHDFS(String tmpFolderForHDFS) {
        this.tmpFolderForHDFS = tmpFolderForHDFS;
    }

    private  String tmpFolderForHive;
    public String getTmpFolderForHive() {
        return this.tmpFolderForHive;
    }
    public void setTmpFolderForHive(String tmpFolderForHive) {
        this.tmpFolderForHive = tmpFolderForHive;
    }

    private  String tmpFolderForMysql;
    public String getTmpFolderForMysql() {
        return this.tmpFolderForMysql;
    }
    public void setTmpFolderForMysql(String tmpFolderForMysql) {
        this.tmpFolderForMysql = tmpFolderForMysql;
    }

    private  String tmpFolderForFile;
    public String getTmpFolderForFile() {
        return this.tmpFolderForFile;
    }
    public void setTmpFolderForFile(String tmpFolderForFile) {
        this.tmpFolderForFile = tmpFolderForFile;
    }

    private String serverAccout;
    public String getServerAccout() {
        return this.serverAccout;
    }
    public void setServerAccout(String serverAccout) {
        this.serverAccout = serverAccout;
    }

    private String serverIp;
    public String getServerIp() {
        return this.serverIp;
    }
    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }


    private String dwScriptPath;
    public String getDwScriptPath() {
        return this.dwScriptPath;
    }
    public void setDwScriptPath(String dwScriptPath) {
        this.dwScriptPath = dwScriptPath;
    }

    private String dwLoggerForHiveXml;
    public String getDwLoggerForHiveXml () {
        return this.dwLoggerForHiveXml;
    }
    public void setDwLoggerForHiveXml (String dwLoggerForHiveXml) {
        this.dwLoggerForHiveXml = dwLoggerForHiveXml;
    }

    private String dwMasterProperties;
    public String getDwMasterProperties() {
        return this.dwMasterProperties;
    }
    public void setDwMasterProperties(String dwMasterProperties) {
        this.dwMasterProperties = dwMasterProperties;
    }

    private String dwHiveBinPath;
    public String getDwHiveBinPath() {
        return this.dwHiveBinPath;
    }
    public void setDwHiveBinPath(String dwHiveBinPath) {
        this.dwHiveBinPath = dwHiveBinPath;
    }

    public void begin(String themeName, String moduleName)
    {
        summaryRunner.begin(themeName, moduleName);
    }

    public void end(String themeName, String moduleName)
    {
        summaryRunner.end(themeName, moduleName);
    }

    public void setTestMode()
    {
        testMode = true;
    }

    public boolean isTestMode()
    {
        return testMode;
    }

    public List<List<String>> runHQLQueryFromCMD(String hql, String queryName,
            String dealDate, String themeName, String moduleName)
    {
        if(hql.trim().equals(""))
            return Collections.emptyList();

        //去注释
        hql = SQLParseUtil.removeComments(hql);

        long startTime = System.currentTimeMillis();

        //替换
        hql = hql.replace("${dealDate}", "'" + dealDate + "'")
                    .replace(PlaceHolders.MONTH_ID, "'" + DWDateUtil.getMonthId(dealDate, true) + "'")
                    .replace(PlaceHolders.MONTH_BEGIN, "'" + DWDateUtil.getMonthBegin(dealDate) + "'")
                    .replace(PlaceHolders.MONTH_END, "'" + DWDateUtil.getMonthEnd(dealDate) + "'")
                    .replace(PlaceHolders.WEEK_ID, "'" + DWDateUtil.getWeekIdEn(dealDate) + "'")
                    .replace(PlaceHolders.WEEK_BEGIN, "'" + DWDateUtil.getWeekBeginDtEn(dealDate) + "'")
                    .replace(PlaceHolders.WEEK_END, "'" + DWDateUtil.getWeekEndDtEn(dealDate) + "'")
                    .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(dealDate))
                    .replace(PlaceHolders.SEVEN_DAYS_BEFORE, DWDateUtil.addDays(dealDate, -7));

        hql = hql.replace(PlaceHolders.File_SUFFIX, dealDate);
        hql = hql.replace(PlaceHolders.TMP_CATALOG, tempCatalog);

        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger)ac.getBean("loggerHive");

        try
        {
            DefaultExecutor executor = new DefaultExecutor();

            executor.setExitValues(null);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream,errorStream);
            executor.setStreamHandler(streamHandler);

            String qName = this.getTmpFolderForHive() + moduleName
                                               + queryName.replace(" ", "_") + ".q";
            PrintWriter output = new PrintWriter(new FileWriter(qName));
            output.println(hql);
            output.close();

            System.out.println(hql);

            executor.execute(CommandLine.parse(
                    this.getDwHiveBinPath() + "/hive -f " + qName));

            String stderr = errorStream.toString();
            if(!stderr.contains("FAILED:"))
            {
                logger.log(startTime, themeName, moduleName,
                        SQLParseUtil.getSqlAction(hql), "RunStop",
                        SQLParseUtil.getTableName(hql), "OK");
            }
            else
            {
                stderr.substring(stderr.indexOf("FAILED:"));
                logger.log(startTime, themeName, moduleName,
                        SQLParseUtil.getSqlAction(hql), (testMode ? "StopRun" : "Exception"),
                        SQLParseUtil.getTableName(hql),
                        stderr.substring(stderr.indexOf("FAILED:")));
                System.out.println(stderr.substring(stderr.indexOf("FAILED:")));
            }

            String queryResult = outputStream.toString();

            String[] rows = queryResult.split("\n");
            List<List<String>> res = new ArrayList<List<String>>();

            for(String row : rows)
            {
               List<String> rowRes = new ArrayList<String>();

               String[] cells = row.split("\t");
               for(String cell : cells)
                   rowRes.add(cell);

               res.add(rowRes);
            }

            return res;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.log(startTime, themeName, moduleName,
                    SQLParseUtil.getSqlAction(hql),
                    (testMode ? "StopRun" : "Exception"),
                    SQLParseUtil.getTableName(hql), e.getMessage());

            return Collections.emptyList();
        }
    }

    /*同一个job内有并发运行两个以上sql的要调用这个方法*/
    public void runHQLFromCMDConcurrent(String hqlList, String sqlFileName,
            String dealDate, String themeName, String moduleName)
    {
        String hqlParamSetBuffer = "";
        String paramSql = "";

        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger)ac.getBean("loggerHive");

        String[] hqls = SQLParseUtil.removeComments(hqlList).split(";");

        for (String hql : hqls)
        {

            if(hql.trim().equals(""))
                continue;

            long startTime = System.currentTimeMillis();

            hql = hql.replace("${dealDate}", "'" + dealDate + "'")
                        .replace(PlaceHolders.MONTH_ID, "'" + DWDateUtil.getMonthId(dealDate, true) + "'")
                        .replace(PlaceHolders.MONTH_BEGIN, "'" + DWDateUtil.getMonthBegin(dealDate) + "'")
                        .replace(PlaceHolders.MONTH_END, "'" + DWDateUtil.getMonthEnd(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_ID, "'" + DWDateUtil.getWeekIdEn(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_BEGIN, "'" + DWDateUtil.getWeekBeginDtEn(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_END, "'" + DWDateUtil.getWeekEndDtEn(dealDate) + "'")
                        .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(dealDate))
                        .replace(PlaceHolders.SEVEN_DAYS_BEFORE, DWDateUtil.addDays(dealDate, -7));
            hql = hql.replace(PlaceHolders.File_SUFFIX, dealDate);
            hql = hql.replace(PlaceHolders.TMP_CATALOG, tempCatalog);

            paramSql = hql.trim().toLowerCase();
            if(paramSql.startsWith("set hive") || paramSql.startsWith("add jar")
                    || paramSql.startsWith("create temporary function")
                    || paramSql.startsWith("set mapred") || paramSql.startsWith("set dfs")
                    || paramSql.startsWith("use"))
            {
                hqlParamSetBuffer += (hql.trim() + ";");
                continue;
            }

            try
            {
                DefaultExecutor executor = new DefaultExecutor();

                executor.setExitValues(null);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
                TeeOutputStream teeErrorStream = new TeeOutputStream(new UnclosableOutputStream(System.err), errorStream);
                PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, teeErrorStream);
                executor.setStreamHandler(streamHandler);

                // LZOINDEXER /path/to/be/indexed/;
                Matcher lzoIndexerMatcher = ptrnLzoIndexer.matcher(hql);
                if (lzoIndexerMatcher.find()) {
                    CommandLine lzoIndexerCommand = new CommandLine(this.getDwScriptPath() + "/lzoindexer.sh");
                    lzoIndexerCommand.addArgument(lzoIndexerMatcher.group(1));
                    System.out.println(lzoIndexerCommand);
                    executor.execute(lzoIndexerCommand);
                    logger.log(startTime, themeName, moduleName,
                            SQLParseUtil.getSqlAction(hql), "RunStop",
                            SQLParseUtil.getTableName(hql), "OK");
                    continue;
                }
                
                String qName = this.getTmpFolderForHive() + moduleName + sqlFileName + ".q";

                PrintWriter output = new PrintWriter(new FileWriter(qName));
                output.print(hqlParamSetBuffer + hql + ";");
                output.close();

                executor.execute(CommandLine.parse(this.getDwHiveBinPath() + "/hive -f " + qName));

                new File(qName).delete();

                String stderr = errorStream.toString();
                if(!stderr.contains("FAILED:"))
                {
                    logger.log(startTime, themeName, moduleName,
                            SQLParseUtil.getSqlAction(hql), "RunStop",
                            SQLParseUtil.getTableName(hql), "OK");
                }
                else
                {
                    stderr.substring(stderr.indexOf("FAILED:"));
                    logger.log(startTime, themeName, moduleName,
                            SQLParseUtil.getSqlAction(hql), (testMode ? "StopRun" : "Exception"),
                            SQLParseUtil.getTableName(hql),
                            stderr.substring(stderr.indexOf("FAILED:")));
                    System.out.println(stderr.substring(stderr.indexOf("FAILED:")));
                }


            }
            catch (Exception e)
            {
                e.printStackTrace();
                logger.log(startTime, themeName, moduleName,
                        SQLParseUtil.getSqlAction(hql),
                        (testMode ? "StopRun" : "Exception"),
                        SQLParseUtil.getTableName(hql), e.getMessage());
            }

        }
    }

    public void runHQLFromCMD(String hqlList, String dealDate,
            String themeName, String moduleName)
    {
        runHQLFromCMDConcurrent(hqlList, "", dealDate, themeName, moduleName);
    }

    public void runHQLFromCMD(String hqlList, String themeName, String moduleName)
    {
        runHQLFromCMD( hqlList, "0000-00-00", themeName, moduleName);
    }

    public void runBatchDropFromCMD(String hqlList, String themeName, String moduleName)
    {
        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger) ac.getBean("loggerHive");

        /* 去掉注释 */
        hqlList = SQLParseUtil.removeComments(hqlList);

        long startTime = System.currentTimeMillis();

        try
        {
            DefaultExecutor executor = new DefaultExecutor();

            executor.setExitValues(null);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
            PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
            executor.setStreamHandler(streamHandler);

            String qName = this.getTmpFolderForHive() + moduleName + ".q";
            PrintWriter output = new PrintWriter(new FileWriter(qName));
            output.print(hqlList);
            output.close();

            System.out.println(hqlList);
            executor.execute(CommandLine.parse(this.getDwHiveBinPath() + "/hive -f " + qName));

            String stderr = errorStream.toString();
            if (!stderr.contains("FAILED:"))
            {
                logger.log(startTime, themeName, moduleName, "drop tmp tables", "RunStop",
                        "drop tmp tables", "OK");
            }
            else
            {
                stderr.substring(stderr.indexOf("FAILED:"));
                logger.log(startTime, themeName, moduleName, "drop tmp tables",
                        (testMode ? "StopRun" : "Exception"), "drop tmp tables",
                        stderr.substring(stderr.indexOf("FAILED:")));
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.log(startTime, themeName, moduleName, "drop tmp tables", (testMode ? "StopRun"
                    : "Exception"), "drop tmp tables", e.getMessage());
        }
    }

    //用于summary表的直接导入，不过前面需要额外执行delete
    public void dumpHiveToMySQLFromCMD(String mysqlTable, String hiveTable,
            String themeName, String moduleName)
    {
        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger) ac.getBean("loggerHive");

        //导出部分
        long startTime = System.currentTimeMillis();

        DefaultExecutor executor = new DefaultExecutor();

        String fNameHive = this.getTmpFolderForHive() + moduleName + ".out";
        String fNameMysql = this.getTmpFolderForMysql() + moduleName + ".out";
        String outCMD = this.getDwScriptPath() + "/exportHiveTableETL.sh " + hiveTable + " " + fNameHive;

        try
        {
            //Properties p = new Properties();
            //FileInputStream pInStream = new FileInputStream(this.getDwMasterProperties());
            //p.load(pInStream);
            //pInStream.close();
            //String myIP = p.getProperty("remote.ip");
            //#System.out.println(myIP);

            String rsyncCMD = "rsync -azv " + fNameHive + " " + this.getServerAccout() + "@" + this.getServerIp() + ":" + this.getTmpFolderForMysql();

            System.out.println(outCMD);
            executor.execute(CommandLine.parse(outCMD));
            System.out.println(rsyncCMD);
            executor.execute(CommandLine.parse(rsyncCMD));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.log(startTime, themeName, moduleName, "dump", (testMode ? "StopRun"
                    : "Exception"), hiveTable, e.getMessage());
        }

        //导入部分
        summaryRunner.runSummarySql("load data infile '" + fNameMysql + "' into table " + mysqlTable,
                themeName, moduleName);
    }

    //临时表专用
    public void dumpOverwriteHiveToMySQLFromCMD(String table, String themeName, String moduleName)
    {
        summaryRunner.runSummarySql("truncate table " + table, themeName, moduleName);
        dumpHiveToMySQLFromCMD(table, table,themeName,moduleName);
    }

    public void dumpOverwriteMySQLToHiveFromCMD(String table, String dealDate, String themeName, String moduleName)
    {
        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger) ac.getBean("loggerHive");

        long startTime = System.currentTimeMillis();

        DefaultExecutor executor = new DefaultExecutor();

        String fNameHive = this.getTmpFolderForHive() + moduleName + ".out";
        String fNameMysql = this.getTmpFolderForMysql() + moduleName + ".out";
        String loadCMD = this.getDwScriptPath() + "/loadHiveTableETL.sh " + table + " " + fNameHive;

        try
        {
            //Properties p = new Properties();
            //FileInputStream pInStream = new FileInputStream(this.getDwMasterProperties());
            //p.load(pInStream);
            //pInStream.close();
            //String myIP = p.getProperty("remote.ip");
            //System.out.println(myIP);

            //导出部分
            String removeCMD = this.getDwScriptPath() +  "/removeMysqlFileETL.sh " + this.getServerIp() + " " + fNameMysql;

            System.out.println(removeCMD);
            executor.execute(CommandLine.parse(removeCMD));

            extractRunner.runExtractSql("select * from " + table + " into outfile '" + fNameMysql +
                    "' fields terminated by '\t'", dealDate,
                    themeName, moduleName);

            //导入部分
            String rsyncCMD = "rsync -azv " + " "+ this.getServerAccout() + "@" + this.getServerIp() + ":" + fNameMysql + " " + this.getTmpFolderForHive();

            System.out.println(rsyncCMD);
            executor.execute(CommandLine.parse(rsyncCMD));
            System.out.println(loadCMD);
            executor.execute(CommandLine.parse(loadCMD));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.log(startTime, themeName, moduleName, "dump", (testMode ? "StopRun"
                    : "Exception"), table, e.getMessage());
        }
    }

    /**
     * 使用自定义SQL将Hive中的表导入MySQL。
     *
     * @param taskName   任务名称，并发调用时必须指定
     * @param hiveSql    Hive导出用SQL
     * @param mysqlSql   MySQL导入用SQL，使用${dataFile}表示数据文件
     * @param mysqlLock  对MySQL的操作可以串行化，无需串行的可传入null。
     * @param themeName
     * @param moduleName
     */
    public void dumpHiveToMySQLCustom(String taskName, String hiveSql, String mysqlSql, String dealDate,
            Object mysqlLock, String themeName, String moduleName) {

        String hiveSqlFile = String.format("%s%s__%s.sql", this.getTmpFolderForHive(), moduleName, taskName);
        String hiveDataFile = String.format("%s%s__%s.txt", this.getTmpFolderForHive(), moduleName, taskName);
        String mysqlDataFile = String.format("%s%s__%s.txt", this.getTmpFolderForMysql(), moduleName, taskName);

        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger) ac.getBean("loggerHive");
        DefaultExecutor executor = new DefaultExecutor();
        long startTime;

        try {

            // export
            hiveSql = hiveSql.replace("${dealDate}", "'" + dealDate + "'")
                        .replace(PlaceHolders.MONTH_ID, "'" + DWDateUtil.getMonthId(dealDate, true) + "'")
                        .replace(PlaceHolders.MONTH_BEGIN, "'" + DWDateUtil.getMonthBegin(dealDate) + "'")
                        .replace(PlaceHolders.MONTH_END, "'" + DWDateUtil.getMonthEnd(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_ID, "'" + DWDateUtil.getWeekIdEn(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_BEGIN, "'" + DWDateUtil.getWeekBeginDtEn(dealDate) + "'")
                        .replace(PlaceHolders.WEEK_END, "'" + DWDateUtil.getWeekEndDtEn(dealDate) + "'")
                        .replace(PlaceHolders.MONTH_ONLY_SUFFIX, DWDateUtil.getMonthOnly(dealDate))
                        .replace(PlaceHolders.SEVEN_DAYS_BEFORE, DWDateUtil.addDays(dealDate, -7));

            FileWriter writer = new FileWriter(hiveSqlFile);
            writer.write(hiveSql);
            writer.close();

            CommandLine hiveCommand = new CommandLine( this.getDwScriptPath() + "/exportHiveTableETLCustom.sh");
            hiveCommand.addArgument(hiveSqlFile);
            hiveCommand.addArgument(hiveDataFile);

            System.out.println(hiveCommand);
            startTime = System.currentTimeMillis();
            executor.execute(hiveCommand);
            logger.log(startTime, themeName, moduleName, "export", "RunStop", taskName, "OK");

            // rsync
//            Properties props = new Properties();
//            props.load(new FileInputStream(this.getDwMasterProperties());
//
//            CommandLine rsyncCommand = new CommandLine("/usr/bin/rsync");
//            rsyncCommand.addArgument("-avz");
//            rsyncCommand.addArgument(hiveDataFile);
//            rsyncCommand.addArgument(String.format("dwadmin@%s:%s", props.get("remote.ip"), mysqlDataFile));
//
//            System.out.println(rsyncCommand);
//            startTime = System.currentTimeMillis();
//            executor.execute(rsyncCommand);
//            logger.log(startTime, themeName, moduleName, "rsync", "RunStop", taskName, "OK");

            // import
            //mysqlSql = mysqlSql.replace("${dataFile}", "'" + mysqlDataFile + "'");
            //因为是本机hiveDataFile没用重命名为mysqlDataFile
            mysqlSql = mysqlSql.replace("${dataFile}", "'" + hiveDataFile + "'");

            if (mysqlLock == null) {
                mysqlLock = new Object();
            }

            synchronized (mysqlLock) {
                summaryRunner.runSummarySql(mysqlSql, dealDate, themeName, moduleName);
            }

        } catch (Exception e) {

            e.printStackTrace();
            logger.log(System.currentTimeMillis(), themeName, moduleName,
                    "dump", "Exception", "", e.getMessage());
        }

    }

    /**
     * 使用自定义SQL将MySQL中的表导入Hive。
     *
     * @param taskName   任务名称，并发调用时必须指定
     * @param mysqlSql   MySQL导出用SQL，使用${dataFile}表示数据文件
     * @param hiveSql    Hive导入用SQL
     * @param compress 是否使用LZO压缩
     * @param themeName
     * @param moduleName
     */
    public void dumpMySQLToHiveCustom(String taskName, String mysqlSql, String hiveSql, String dealDate,
            boolean compress, String themeName, String moduleName) {

        String mysqlDataFile = String.format("%s%s__%s.txt", this.getTmpFolderForMysql(), moduleName, taskName);
        String hdfsDataFolder = String.format("%s%s__%s/", this.getTmpFolderForHDFS(), moduleName, taskName);
        String hdfsDataFile = String.format("%s%s__%s.txt", hdfsDataFolder, moduleName, taskName);

        ApplicationContext ac = new ClassPathXmlApplicationContext(this.getDwLoggerForHiveXml());
        DWLogger logger = (DWLogger) ac.getBean("loggerHive");
        DefaultExecutor executor = new DefaultExecutor();
        long startTime;

        try {

            //Properties props = new Properties();
            //props.load(new FileInputStream(this.getDwMasterProperties()));
            CommandLine sshCommand = new CommandLine("/usr/bin/ssh");
            sshCommand.addArgument(this.getServerAccout() + "@" + this.getServerIp());

            // delete
            CommandLine rmCommand = new CommandLine(sshCommand);
            rmCommand.addArgument("rm");
            rmCommand.addArgument("-f");
            rmCommand.addArgument(mysqlDataFile);
            System.out.println(rmCommand);
            executor.execute(rmCommand);

            // export
            mysqlSql = mysqlSql.replace("${dataFile}", "'" + mysqlDataFile + "'");
            extractRunner.runExtractSql(mysqlSql, dealDate, themeName, moduleName);

            // compress
            if (compress) {
                CommandLine lzopCommand = new CommandLine(sshCommand);
                lzopCommand.addArgument("/usr/bin/lzop");
                lzopCommand.addArgument("-U");
                lzopCommand.addArgument("-f");
                lzopCommand.addArgument(mysqlDataFile);

                startTime = System.currentTimeMillis();
                System.out.println(lzopCommand);
                executor.execute(lzopCommand);
                logger.log(startTime, themeName, moduleName, "lzop", "RunStop", taskName, "OK");

                mysqlDataFile += ".lzo";
                hdfsDataFile += ".lzo";
            }

            // copy from local to hdfs
            CommandLine hdfsCommand = new CommandLine(sshCommand);
            //在 shell 中写
            hdfsCommand.addArgument(this.getDwScriptPath() + "/copy-from-local.sh");
            hdfsCommand.addArgument(mysqlDataFile);
            hdfsCommand.addArgument(hdfsDataFile);

            startTime = System.currentTimeMillis();
            System.out.println(hdfsCommand);
            executor.execute(hdfsCommand);
            logger.log(startTime, themeName, moduleName, "copyFromLocal", "RunStop", taskName, "OK");

            // index
            if (compress) {
                CommandLine indexCommand = new CommandLine(this.getDwScriptPath() + "/lzoindexer.sh");
                indexCommand.addArgument(hdfsDataFolder);

                startTime = System.currentTimeMillis();
                System.out.println(indexCommand);
                executor.execute(indexCommand);
                logger.log(startTime, themeName, moduleName, "lzo-indexer", "RunStop", taskName, "OK");
            }

            // import
            hiveSql = hiveSql.replace("${dataFile}", "'" + hdfsDataFolder + "'");
            runHQLFromCMDConcurrent(hiveSql, taskName, dealDate, themeName, moduleName);

        } catch (Exception e) {
            e.printStackTrace();
            logger.log(System.currentTimeMillis(), themeName, moduleName,
                    "dump", "Exception", "", e.getMessage());
        }

    }

    //injection
    public void setSummaryRunner(SummaryRunner summaryRunner)
    {
        this.summaryRunner = summaryRunner;
    }

    public SummaryRunner getSummaryRunner()
    {
        return summaryRunner;
    }

    public void setExtractRunner(ExtractRunner extractRunner) {
        this.extractRunner = extractRunner;
    }

    public ExtractRunner getExtractRunner() {
        return extractRunner;
    }

    public void setTempCatalog(String tempCatalog)
    {
        this.tempCatalog = tempCatalog;
    }

    public String getTempCatalog()
    {
        return tempCatalog;
    }

}

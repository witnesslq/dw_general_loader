package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.angejia.dw.DWLogger;
import com.angejia.dw.ExtractRunner;
import com.angejia.dw.InputArgs;
import com.angejia.dw.LoadRunner;
import com.angejia.dw.hive.HiveRunner;


public class EtlTask extends AbstractTask {

    private ServerType serverType;

    public EtlTask(ApplicationContext context, Task task, InputArgs inputs, ServerType serverType) {
        super(context, task, inputs);
        this.serverType = serverType;
    }

    @Override
    protected void runInternal() throws Exception {

        JSONObject details = JSONObject.fromObject(task.getDetails());
        JSONObject source = details.getJSONObject("source");
        JSONObject target = details.getJSONObject("target");

        switch (source.getInt("type")) {
        case Task.SOURCE_ETL_SLAVE:

            ExtractInfo extractInfo = extractEtlSlave(source, task.getModuleName());

            switch (target.getInt("type")) {
            case Task.TARGET_DW_MASTER:
                loadDwMaster(extractInfo, task.getModuleName(), target);
                break;

            case Task.TARGET_HIVE:
                loadHive(extractInfo, task.getModuleName(), target);
                break;

            default:
                throw new Exception("Not implemented.");
            }

            break;

        case Task.SOURCE_DW_MASTER:

            switch (target.getInt("type")) {
            case Task.TARGET_HIVE:
                HiveRunner hiveRunner = (HiveRunner) context.getBean("hiveRunner");
                hiveRunner.dumpMySQLToHiveCustom("default",
                        generateMysqlExport(source), generateHiveImport(target),
                        inputs.getStartDay(), target.getBoolean("compress"),
                        task.getModuleName(), task.getModuleName());
                break;

            default:
                throw new Exception("Not implemented.");
            }

            break;

        case Task.SOURCE_HIVE:

            switch (target.getInt("type")) {
            case Task.TARGET_DW_MASTER:
                HiveRunner hiveRunner = (HiveRunner) context.getBean("hiveRunner");
                hiveRunner.dumpHiveToMySQLCustom("default",
                        generateHiveExport(source), generateMysqlImport(target),
                        inputs.getStartDay(), null,
                        task.getModuleName(), task.getModuleName());
                break;

            default:
                throw new Exception("Not implemented.");
            }

            break;

        default:
            throw new Exception("Not implemented.");
        }

    }

    private ExtractInfo extractEtlSlave(JSONObject source, String moduleName) throws Exception {
        
        // init extract runner
        System.setProperty("dw_general_loader_properties", source.getString("config"));
        //ApplicationContext etlSlaveContext = new ClassPathXmlApplicationContext("/applicationContext.xml");
        //Properties databaseProperties = (Properties) etlSlaveContext.getBean("databaseProperties");
        //String ip = databaseProperties.getProperty("remote.ip");

        ExtractRunner extractRunner = (ExtractRunner) this.context.getBean("extractRunner");
        HiveRunner hiveRunner = (HiveRunner) this.context.getBean("hiveRunner");
        String ip = hiveRunner.getServerIp();
        String account = hiveRunner.getServerAccout();

        // delete data file
        String dataFile = String.format("%s%s%s.sql",
                extractRunner.getTempCatalog(), moduleName, inputs.getStartDay());

        DefaultExecutor executor = new DefaultExecutor();
        CommandLine cmd = new CommandLine("/usr/bin/ssh");
        if (serverType == ServerType.HIVE) {
            //cmd.addArgument("-i").addArgument("/home/hadoop/.ssh/dwadmin");
            cmd.addArgument(String.format(account + "@%s", ip));
        } else {
            cmd.addArgument(ip);
        }
        cmd.addArgument("rm").addArgument("-f").addArgument(dataFile);
        System.out.println(cmd);
        executor.execute(cmd);

        // run extract sql
        String sql = generateMysqlExport(source)
                .replace("${dataFile}", "'" + dataFile + "'");
        extractRunner.runExtractSql(sql, inputs.getStartDay(), moduleName, moduleName);

        ExtractInfo extractInfo = new ExtractInfo();
        extractInfo.setDataFile(dataFile);
        extractInfo.setIp(ip);
        return extractInfo;
    }

    private void loadDwMaster(ExtractInfo extractInfo, String moduleName, JSONObject target)
            throws Exception {

        LoadRunner loadRunner = (LoadRunner) context.getBean("loadRunner");

        String slaveDataFile = extractInfo.getDataFile();
        String masterDataFile = String.format("%s%s%s.sql",
                loadRunner.getTempCatalog(), moduleName, inputs.getStartDay());

        // rsync
//        DefaultExecutor executor = new DefaultExecutor();
//
//        CommandLine cmd = new CommandLine("/usr/bin/rsync");
//        cmd.addArgument("-vW");
//        cmd.addArgument(String.format("%s:%s", extractInfo.getIp(), slaveDataFile));
//        cmd.addArgument(masterDataFile);
//
//        long startTime = System.currentTimeMillis();
//        System.out.println(cmd);
//        executor.execute(cmd);
//        loadRunner.getLogger().log(startTime, moduleName, moduleName,
//                "rsync", "RunStop", slaveDataFile, "OK");

        // load
        String sql = generateMysqlImport(target)
                .replace("${dataFile}", "'" + masterDataFile + "'");
        loadRunner.runLoadSql(sql, inputs.getStartDay(), moduleName, moduleName);
    }

    private void loadHive(ExtractInfo extractInfo, String moduleName, JSONObject target)
            throws Exception {

        HiveRunner hiveRunner = (HiveRunner) this.context.getBean("hiveRunner");
        DWLogger logger = hiveRunner.getSummaryRunner().getLogger();
        
        String ip = hiveRunner.getServerIp();
        String account = hiveRunner.getServerAccout();
        String dwScriptPath = hiveRunner.getDwScriptPath();

        String mysqlDataFile = extractInfo.getDataFile();
        String hdfsDataFolder = String.format("%s%s/", hiveRunner.getTmpFolderForHDFS(), moduleName);
        String hdfsDataFile = String.format("%s%s.txt", hdfsDataFolder, moduleName);

        DefaultExecutor executor = new DefaultExecutor();
        CommandLine sshCommand = new CommandLine("/usr/bin/ssh");
        //sshCommand.addArgument(String.format(hiveRunner.getServerAccout() + "@%s", extractInfo.getIp()));
        sshCommand.addArgument(String.format(account + "@%s", ip));

        long startTime;

        // compress
        if (target.getBoolean("compress")) {

            CommandLine lzopCommand = new CommandLine(sshCommand);
            //lzopCommand.addArgument("/home/dwadmin/dwetl/lzop");
            lzopCommand.addArgument(dwScriptPath);
            
            lzopCommand.addArgument("-U");
            lzopCommand.addArgument("-f");
            lzopCommand.addArgument(mysqlDataFile);

            startTime = System.currentTimeMillis();
            System.out.println(lzopCommand);
            executor.execute(lzopCommand);
            logger.log(startTime, moduleName, moduleName, "lzop", "RunStop", mysqlDataFile, "OK");

            mysqlDataFile += ".lzo";
            hdfsDataFile += ".lzo";
        }

        // copy from local slave
        CommandLine hdfsCommand = new CommandLine(sshCommand);
        hdfsCommand.addArgument(dwScriptPath + "/copy-from-local.sh");//load hdfs
        hdfsCommand.addArgument(mysqlDataFile);
        hdfsCommand.addArgument(hdfsDataFile);

        startTime = System.currentTimeMillis();
        System.out.println(hdfsCommand);
        executor.execute(hdfsCommand);
        logger.log(startTime, moduleName, moduleName, "copyFromLocal", "RunStop", mysqlDataFile, "OK");

        // index
        if (target.getBoolean("compress")) {
            CommandLine indexCommand = new CommandLine(dwScriptPath + "lzoindexer.sh");
            indexCommand.addArgument(hdfsDataFolder);

            startTime = System.currentTimeMillis();
            System.out.println(indexCommand);
            executor.execute(indexCommand);
            logger.log(startTime, moduleName, moduleName, "lzo-indexer", "RunStop", hdfsDataFolder, "OK");
        }

        // import
        String sql = generateHiveImport(target)
                .replace("${dataFile}", "'" + hdfsDataFolder + "'");
        hiveRunner.runHQLFromCMDConcurrent(sql, inputs.getStartDay(), inputs.getStartDay(), moduleName, moduleName);

    }

    public static String generateMysqlImport(JSONObject target) throws Exception {

        StringBuilder mysqlSql = new StringBuilder();
        switch (target.getInt("loadType")) {

        case Task.MYSQL_STANDARD:

            // delete
            String where = target.getString("where").trim();
            if (!where.isEmpty()) {
                mysqlSql.append("DELETE FROM `");
                mysqlSql.append(target.getString("database"));
                mysqlSql.append("`.`").append(target.getString("table")).append("`");
                mysqlSql.append(" WHERE ").append(where).append(";\n");
            } else {
                mysqlSql.append("TRUNCATE TABLE `");
                mysqlSql.append(target.getString("database"));
                mysqlSql.append("`.`").append(target.getString("table")).append("`");
                mysqlSql.append(";\n");
            }

            // load
            mysqlSql.append("LOAD DATA LOCAL INFILE ${dataFile} INTO TABLE `");
            mysqlSql.append(target.getString("database"));
            mysqlSql.append("`.`").append(target.getString("table")).append("`");
            mysqlSql.append(" CHARACTER SET UTF8 ");

            // fields
            List<MysqlField> mysqlFields = parseMysqlFields(target.getJSONArray("fields"));
            List<String> fieldLines = new ArrayList<String>();
            if (mysqlFields != null) {
                for (MysqlField field : mysqlFields) {
                    if (!field.getLoad()) {
                        continue;
                    }
                    fieldLines.add(String.format("  `%s`", field.getName()));
                }
            }

            if (!fieldLines.isEmpty()) {
                mysqlSql.append(" (\n");
                mysqlSql.append(StringUtils.join(fieldLines, ",\n"));
                mysqlSql.append("\n)");
            }

            mysqlSql.append(";\n");
            break;

        case Task.MYSQL_CUSTOM:
            String sql = target.getString("sql").trim();
            if (sql.isEmpty()) {
                throw new Exception("MySQL custom SQL is empty.");
            }
            mysqlSql.append(sql);
            break;

        default:
            throw new Exception("Invalid MySQL type.");
        }

        return mysqlSql.toString();
    }

    private String generateMysqlExport(JSONObject source) throws Exception {

        StringBuilder mysqlSql = new StringBuilder();
        switch (source.getInt("extractType")) {

        case Task.MYSQL_STANDARD:

            mysqlSql.append("SELECT");

            // fields
            List<MysqlField> mysqlFields = parseMysqlFields(source.getJSONArray("fields"));
            List<String> fieldLines = new ArrayList<String>();
            if (mysqlFields != null) {
                for (MysqlField field : mysqlFields) {
                    if (!field.getExtract()) {
                        continue;
                    }
                    if (field.getCleanse()) {
                        fieldLines.add(String.format(
                                "  REPLACE(REPLACE(REPLACE(`%s`, '\\t', ' '), '\\r', ''), '\\n', ' ')",
                                field.getName()));
                    } else {
                        fieldLines.add(String.format("  `%s`", field.getName()));
                    }
                }
            }

            if (fieldLines.isEmpty()) {
                mysqlSql.append(" *\n");
            } else {
                mysqlSql.append("\n");
                mysqlSql.append(StringUtils.join(fieldLines, ",\n"));
                mysqlSql.append("\n");
            }

            // table
            mysqlSql.append("FROM `").append(source.getString("database"));
            mysqlSql.append("`.`").append(source.getString("table")).append("`\n");

            // where
            String where = source.getString("where").trim();
            if (!where.isEmpty()) {
                mysqlSql.append("WHERE ").append(where).append("\n");
            }

            // outfile
            mysqlSql.append("INTO OUTFILE ${dataFile};");

            break;

        case Task.MYSQL_CUSTOM:
            String sql = source.getString("sql").trim();
            if (sql.isEmpty()) {
                throw new Exception("MySQL custom SQL is empty.");
            }
            mysqlSql.append(sql);
            break;

        default:
            throw new Exception("Invalid MySQL type.");
        }

        return mysqlSql.toString();
    }

    private String generateHiveImport(JSONObject target) throws Exception {

        StringBuilder hiveSql = new StringBuilder();
        switch (target.getInt("loadType")) {
        case Task.HIVE_STANDARD:

            String partition = target.getString("partition").trim();

            if (!partition.isEmpty()) {
                hiveSql.append("USE ").append(target.getString("database")).append(";");
                hiveSql.append("ALTER TABLE ").append(target.getString("table"));
                hiveSql.append(" DROP IF EXISTS PARTITION (").append(partition).append(" = ${dealDate});");
            }

            hiveSql.append("LOAD DATA INPATH ${dataFile} \n");
            hiveSql.append("OVERWRITE INTO TABLE ").append(target.getString("database"));
            hiveSql.append(".").append(target.getString("table"));

            if (!partition.isEmpty()) {
                hiveSql.append("\nPARTITION (" + partition + " = ${dealDate})");
            }

            hiveSql.append(";");
            break;

        case Task.HIVE_CUSTOM:
            String sql = target.getString("sql").trim();
            if (sql.isEmpty()) {
                throw new Exception("Hive custom SQL is empty.");
            }
            hiveSql.append(sql);
            break;

        default:
            throw new Exception("Invalid Hive type.");
        }

        return hiveSql.toString();
    }

    public static String generateHiveExport(JSONObject source) throws Exception {

        StringBuilder hiveSql = new StringBuilder();
        switch (source.getInt("extractType")) {
        case Task.HIVE_STANDARD:
            hiveSql.append("SELECT * FROM ").append(source.getString("database"));
            hiveSql.append(".").append(source.getString("table"));

            String where = source.getString("where").trim();
            if (!where.isEmpty()) {
                hiveSql.append("\nWHERE ").append(where);
            }

            hiveSql.append(";");
            break;

        case Task.HIVE_CUSTOM:
            String sql = source.getString("sql").trim();
            if (sql.isEmpty()) {
                throw new Exception("Hive custom SQL is empty.");
            }
            hiveSql.append(sql);
            break;

        default:
            throw new Exception("Invalid Hive type.");
        }

        return hiveSql.toString();
    }

    private static List<MysqlField> parseMysqlFields(JSONArray fieldArray) throws JSONException {

        if (fieldArray == null || fieldArray.isEmpty()) {
            return null;
        }

        List<MysqlField> fieldList = new ArrayList<MysqlField>();
        for (int i = 0; i < fieldArray.size(); ++i) {
            JSONObject fieldInfo = fieldArray.getJSONObject(i);
            MysqlField field = new MysqlField();
            field.setName(fieldInfo.getString("name"));
            field.setType(fieldInfo.getString("type"));
            field.setExtract(fieldInfo.optBoolean("extract", false));
            field.setCleanse(fieldInfo.optBoolean("cleanse", false));
            field.setLoad(fieldInfo.optBoolean("load", false));
            fieldList.add(field);
        }

        return fieldList;
    }

}

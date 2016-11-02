package main;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.angejia.dw.InputArgs;
import com.angejia.dw.hive.HiveRunner;
import com.angejia.dw.util.ArgsParseUtil;

public class MainClass implements ApplicationContextAware {

    private static final String DW_MATER_PROPERTIES = "dw-master.properties";

    private ApplicationContext context;
    private Properties hadoopProperties;
    private ServerType serverType;

    public int run(String[] args) {
        
        try {

            // parse arguments
            List<String> argList = new LinkedList<String>();
            for (String arg : args) {
                argList.add(arg);
            }

            if (argList.size() < 1) {
                usage();
                return 1;
            }

            // get task
            Task task = null;
            try {
                task = fetchTask(Integer.parseInt(argList.get(0)));
            } catch (NumberFormatException e) {
                task = fetchTask(argList.get(0));
            }

            if (task == null) {
                throw new Exception("Task not found.");
            }

            argList.remove(0);

            // server
//            String username = System.getProperty("user.name");
//            if (username.equals("dwadmin")) {
//                serverType = ServerType.DW_MASTER;
//            } else if (username.equals("hadoop")) {
                //只运行 Hql
                serverType = ServerType.HIVE;
//            } else {
//                throw new Exception("Unknown server.");
//            }

            // parse job arguments
            InputArgs inputs = ArgsParseUtil.parseArgs(argList.toArray(new String[0]), "");
            
            //type 1:收据抽取 2:Mysql脚本 3:Hive脚本
            System.out.println("type: " + task.getType());
            System.out.println("module name : " + task.getModuleName());
            System.out.println("details: " + task.getDetails());
            //System.exit(0);

            // run
            AbstractTask taskRunner;
            switch (task.getType()) {
                
                case Task.TYPE_ETL:
    
                    //JSONObject details = JSONObject.fromObject(task.getDetails());
                    //int sourceType = details.getJSONObject("source").getInt("type");
                    //int targetType = details.getJSONObject("target").getInt("type");
    
                    /*if (sourceType == Task.SOURCE_HIVE
                            || targetType == Task.TARGET_HIVE) {
    
                        if (serverType != ServerType.HIVE) {
                            return runOnHive(task, args);
                        }
    
                    }*/
    
                    taskRunner = new EtlTask(context, task, inputs, serverType);
    
                    break;
    
                case Task.TYPE_MYSQL:
                    if (serverType != ServerType.DW_MASTER) {
                        throw new Exception("Wrong server.");
                    }
                    taskRunner = new MysqlTask(context, task, inputs);
                    break;
    
                case Task.TYPE_HIVE:
                    if (serverType != ServerType.HIVE) {
                        return runOnHive(task, args);
                    }
                    taskRunner = new HiveTask(context, task, inputs);
                    break;
    
                default:
                    throw new Exception("Invalid type.");
            }

            return taskRunner.run();

        } catch (Exception e) {
            e.printStackTrace();
            return 2;
        }
    }

    private Task fetchTask(int taskId) {
        return fetchTask("id", taskId);
    }

    private Task fetchTask(String moduleName) {
        return fetchTask("module_name", moduleName);
    }

    private Task fetchTask(String column, Object value) {

        JdbcTemplate jdbcTemplate = (JdbcTemplate) context.getBean("jdbcTemplateRemote");

        SqlRowSet rs = jdbcTemplate.queryForRowSet(
                "SELECT module_name, type, details"
                + " FROM dw_monitor.dwms_dev_task"
                + " WHERE status = ? AND " + column + " = ?",
                new Object[] { Task.STATUS_EFFECTIVE, value });

        if (!rs.next()) {
            return null;
        }

        Task row = new Task();
        row.setModuleName(rs.getString("module_name"));
        row.setType(rs.getInt("type"));
        row.setDetails(rs.getString("details"));

        return row;
    }

    private int runOnHive(Task task, String[] args) throws Exception {
        HiveRunner hiveRunner = (HiveRunner) this.context.getBean("hiveRunner");
        DefaultExecutor executor = new DefaultExecutor();

        //CommandLine cmd = new CommandLine("/usr/bin/ssh");
        //cmd.addArgument(String.format("hadoop@%s", hadoopProperties.getProperty("hadoop.dw.ip")));
        //cmd.addArgument(String.format("dwadmin@%s", hadoopProperties.getProperty("hadoop.dw.ip")));
        //cmd.addArgument("java");
        CommandLine cmd = new CommandLine("java");
        cmd.addArgument("-Dfile.encoding=UTF-8");
        cmd.addArgument("-jar");
        //cmd.addArgument("/home/dwadmin/dwetl/dw_general_loader.jar");
        cmd.addArgument(hiveRunner.getDwScriptPath() + "/run_jar/dw_general_loader.jar");
        
        for (String arg : args) {
            cmd.addArgument(arg);
        }

        System.out.println(cmd);
        return executor.execute(cmd);
    }

    private void usage() {
        System.err.println("Usage: java -jar dw_general_laoder.jar <taskId> [dealDate] ...");
    }

    public static void main(String[] args) {
        System.setProperty("dw_general_loader_properties", DW_MATER_PROPERTIES);
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");

        //String[] testArgs = new String[2];
        //testArgs[0] = "da_broker_summary_basis_info_daily";
        //testArgs[1] = "2015-08-03";

        System.exit(((MainClass) context.getBean("mainClass")).run(args));
    }

    @Override
    public void setApplicationContext(ApplicationContext context)
            throws BeansException {

        this.context = context;
    }

    public Properties getHadoopProperties() {
        return hadoopProperties;
    }

    public void setHadoopProperties(Properties hadoopProperties) {
        this.hadoopProperties = hadoopProperties;
    }

}

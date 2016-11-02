package main;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.angejia.dw.InputArgs;
import com.angejia.dw.LoadRunner;
import com.angejia.dw.hive.HiveRunner;

public class HiveTask extends AbstractTask {

    public HiveTask(ApplicationContext context, Task task, InputArgs inputs) {
        super(context, task, inputs);
    }

    @Override
    protected void runInternal() throws Exception {

        HiveRunner hiveRunner = (HiveRunner) context.getBean("hiveRunner");
        JSONObject details = JSONObject.fromObject(task.getDetails());

        // run script
        hiveRunner.runHQLFromCMDConcurrent(details.getString("sql"),
                inputs.getStartDay(), inputs.getStartDay(), task.getModuleName(), task.getModuleName());

        // run h2m
        JSONObject h2m = details.optJSONObject("h2m");
        if (h2m != null && !h2m.isNullObject() && h2m.optBoolean("enabled", false)) {
            runH2m(hiveRunner, h2m);
        }
    }

    private void runH2m(HiveRunner hiveRunner, JSONObject h2m) throws Exception {

        JSONObject source = new JSONObject();
        source.put("extractType", Task.HIVE_STANDARD);
        source.put("database", h2m.getString("hiveDatabase"));
        source.put("table", h2m.getString("hiveTable"));
        source.put("where", h2m.getString("hiveWhere"));

        JSONObject target = new JSONObject();
        target.put("loadType", Task.MYSQL_STANDARD);
        target.put("database", h2m.getString("mysqlDatabase"));
        target.put("table", h2m.getString("mysqlTable"));
        target.put("where", h2m.getString("mysqlWhere"));
        target.put("fields", fetchMysqlFields(h2m.getString("mysqlDatabase"), h2m.getString("mysqlTable"), h2m.getString("partition")));

        hiveRunner.dumpHiveToMySQLCustom("default",
                EtlTask.generateHiveExport(source), EtlTask.generateMysqlImport(target),
                inputs.getStartDay(), null,
                task.getModuleName(), task.getModuleName());
    }

    private JSONArray fetchMysqlFields(String database, String table, String partition) throws Exception {

        LoadRunner loadRunner = (LoadRunner) context.getBean("loadRunner");
        SqlRowSet rs = loadRunner.getJdbcTemplate().queryForRowSet(
                "SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                new Object[] { database, table });

        JSONArray fieldList = new JSONArray();
        JSONObject partitionField = null;

        while (rs.next()) {
            JSONObject field = new JSONObject();
            field.put("name", rs.getString("column_name"));
            field.put("type", rs.getString("column_type"));
            field.put("load", true);
            if (rs.getString("column_name").equals(partition)) {
                partitionField = field;
            } else {
                fieldList.add(field);
            }
        }

        if (partitionField != null) {
            fieldList.add(partitionField);
        }

        return fieldList;
    }

}

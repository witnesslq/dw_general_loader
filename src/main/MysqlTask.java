package main;

import net.sf.json.JSONObject;

import org.springframework.context.ApplicationContext;

import com.angejia.dw.InputArgs;
import com.angejia.dw.SummaryRunner;

public class MysqlTask extends AbstractTask {

    public MysqlTask(ApplicationContext context, Task task, InputArgs inputs) {
        super(context, task, inputs);
    }

    @Override
    protected void runInternal() throws Exception {

        JSONObject details = JSONObject.fromObject(task.getDetails());
        SummaryRunner summaryRunner = (SummaryRunner) context.getBean("summaryRunner");
        summaryRunner.runSummarySql(details.getString("sql"),
                inputs.getStartDay(), task.getModuleName(), task.getModuleName());

    }

}

package main;

import org.springframework.context.ApplicationContext;

import com.angejia.dw.DWLogger;
import com.angejia.dw.InputArgs;
import com.angejia.dw.SummaryRunner;

public abstract class AbstractTask {

    protected ApplicationContext context;
    protected Task task;
    protected InputArgs inputs;

    public AbstractTask(ApplicationContext context, Task task, InputArgs inputs) {
        this.context = context;
        this.task = task;
        this.inputs = inputs;
    }

    public int run() {

        SummaryRunner summaryRunner = (SummaryRunner) context.getBean("summaryRunner");
        summaryRunner.begin(task.getModuleName(), task.getModuleName());

        try {
            runInternal();
        } catch (Exception e) {
            e.printStackTrace();
            summaryRunner.getLogger().log(
                    System.currentTimeMillis(), task.getModuleName(), task.getModuleName(),
                    "main", "Exception", "", e.getMessage());
        } finally {
            summaryRunner.end(task.getModuleName(), task.getModuleName());
        }

        return DWLogger.hasException ? 2 : 0;
    }

    protected abstract void runInternal() throws Exception;

}

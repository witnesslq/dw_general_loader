package com.angejia.dw;

/**
 * 该对象存储输入参数的解析结果，包含3个信息：任务名字（完整运行程序时没有这个信息），开始时间和结束时间。
 * @author larrysun
 *
 */
public class InputArgs
{
    private String startDay;
    private String endDay;
    private String[] jobNames;
    
    public InputArgs(String startDay, String endDay, String[] jobNames)
    {
        this.setStartDay(startDay);
        this.setEndDay(endDay);
        this.setJobNames(jobNames);
    }
    
    public InputArgs()
    {
    	
    }

    public void setStartDay(String startDay)
    {
        this.startDay = startDay;
    }

    public String getStartDay()
    {
        return startDay;
    }

    public void setEndDay(String endDay)
    {
        this.endDay = endDay;
    }

    public String getEndDay()
    {
        return endDay;
    }

    public void setJobNames(String[] jobNames)
    {
        this.jobNames = jobNames;
    }

    public String[] getJobNames()
    {
        return jobNames;
    }
    
    public boolean isRunAllJobs()
    {
        return jobNames == null || jobNames.length == 0;
    }
}

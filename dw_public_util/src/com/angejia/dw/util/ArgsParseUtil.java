package com.angejia.dw.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.angejia.dw.InputArgs;
import com.angejia.dw.exception.ParseRuntimeException;
import com.angejia.dw.util.DWDateUtil;

/**
 * <p>解析程序传入的参数。</p>
 * <p>目前支持两种写法，一种是直接输入开始日期/结束日期，</p>
 * <p>另一种是通过选项来指定。</p>
 * <p>选项有4个:</p>
 * <p>-u 显示这个程序的用法，不跑程序</p>
 * <p>-j 需要运行的单个任务，以逗号分隔</p>
 * <p>-s 开始时间</p>
 * <p>-e 结束时间</p>
 * @author larrysun
 *
 */
public class ArgsParseUtil
{
    private static Options options = new Options();
    static
    {
        options.addOption("j", true, "Jobs name");
        options.addOption("s", true, "Start day");
        options.addOption("e", true, "End day");
        options.addOption("u", false, "Show Usage");
    }
    
    /**
     * 解析程序输入参数
     * @param args 主程序(main)的参数
     * @param usage 帮助信息
     * @return 如果参数属于正常范围，返回一个包含输入参数的Object，否则返回null，程序不应继续运行
     * @throws ParseException
     */
    public static InputArgs parseArgs(String[] args, String usage) 
    {
        CommandLineParser parser = new PosixParser();
        CommandLine line = null;
        try
        {
            line = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            throw new ParseRuntimeException(e);
        }
        
        //日期格式的限定(YYYY-MM-DD)
        String regex = "\\d{4}-\\d{2}-\\d{2}";
        Pattern pattern = Pattern.compile(regex);

        //程序使用参数说明
        if (line.hasOption("u")) 
        {
            System.out.println(usage);
            return null;
        }

        //没有job选项参数，按照直接打日期处理，运行所有任务
        if (!line.hasOption("j")) 
        {
            String yesterday = DWDateUtil.getYesterDay();
            
            //无参数，运行昨天
            if(args.length == 0) 
            {
                InputArgs inputArgs = new InputArgs();
                inputArgs.setStartDay(yesterday);
                return inputArgs;
            }
            //单个参数，运行该天
            else if(args.length == 1)
            {
                //startday
                Matcher m1 = pattern.matcher(args[0]);
                
                if(m1.find()) 
                {
                    InputArgs inputArgs = new InputArgs();
                    inputArgs.setStartDay(args[0]);
                    return inputArgs;
                } 
                else 
                {
                    System.out.println("invalid date format. should be YYYY-MM-DD.");
                    return null;
                }
            }
            //两个参数，运行该时间段
            else if(args.length == 2)
            {
                //startday
                Matcher m1 = pattern.matcher(args[0]);
                //endday
                Matcher m2 = pattern.matcher(args[1]);
                
                if(m1.find() && m2.find()) 
                {
                    InputArgs inputArgs = new InputArgs();
                    inputArgs.setStartDay(args[0]);
                    inputArgs.setEndDay(args[1]);
                    return inputArgs;
                } 
                else 
                {
                    System.out.println("invalid date format. should be YYYY-MM-DD.");
                    return null;
                }
            }
            //参数的数量有问题
            else
            {
                return null;
            }
        }
        else
        {
            //如果-j后面不跟参数，视作跑整个程序
            String[] jobs = line.getOptionValue("j").split(",");
            String startday = line.getOptionValue("s");
            String endday = line.getOptionValue("e");
            
            //验证日期格式
            boolean isStartDayInvalid = (startday != null && !pattern.matcher(startday).find());
            boolean isEndDayInvalid = (endday != null && !pattern.matcher(endday).find());
            if(isStartDayInvalid || isEndDayInvalid)
            {
                System.out.println("invalid date format. should be YYYY-MM-DD.");
                return null;
            }
            else
            {
                if(startday == null && endday == null)
                {
                    startday = DWDateUtil.getYesterDay();
                }
                else if(startday == null && endday != null)
                {
                    System.out.println("to run with end date, you must have the start date first.");
                    return null;
                }
                return new InputArgs(startday, endday, jobs);
            }
            
        }
    }
}

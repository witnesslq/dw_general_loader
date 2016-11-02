package com.angejia.dw.exception;

/**
 * 该类作为java.text.ParseException的Runtime版本。
 * 原因是ETL程序不应有错误的日期格式输入，如果有的话可以立即抛出异常停止程序。
 * @author larrysun
 *
 */
public class ParseRuntimeException extends RuntimeException
{
    private static final long serialVersionUID = 7801019223056600555L;
    
    public ParseRuntimeException()
    {
    }

    public ParseRuntimeException(String msg)
    {
      super(msg);
    }

    public ParseRuntimeException(String msg, Throwable t)
    {
      super(msg, t);
    }

    public ParseRuntimeException(Throwable t)
    {
      super(t);
    }
    
}

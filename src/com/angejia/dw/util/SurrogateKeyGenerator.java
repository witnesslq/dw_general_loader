package com.angejia.dw.util;

public class SurrogateKeyGenerator
{
    private static int count = 0;
    
    public SurrogateKeyGenerator()
    {
        count = 0;
    }
    
    public int getNext()
    {
        count++;
        return count;
    }
    
    public void reset()
    {
        count = 0;
    }
}

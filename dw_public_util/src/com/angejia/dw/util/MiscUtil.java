package com.angejia.dw.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MiscUtil
{
    public static void deleteFile(String fileName)
    {
        File file = new File(fileName);
        if (file.exists())
            file.delete();
        
        System.out.println("delete file: " + fileName);
    }
    
    public static List<File> listFilesWithDate(String fileDir, String dealDate)
    {
        List<File> fs = new ArrayList<File>();

        File fileDirectory = new File(fileDir);
        File[] files = fileDirectory.listFiles();
        
        for(File file : files)
            if(file.getName().toLowerCase().contains(dealDate))
                fs.add(file);
      
        return fs;
    }
    
    public static File[] listLogFiles(String fileDir)
    {
        FileFilter filefilter = new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return pathname.getName().toLowerCase().endsWith(".log");
            }
        };

        File fileDirectory = new File(fileDir);
        File[] files = fileDirectory.listFiles(filefilter);
        Arrays.sort(files);
        return files;
    }
    
    /*UTF8的BOM头是windows下面所特有的，用于标示UTF8（windows下默认是认为文件用ansi），linux下面不用（因为他默认用utf8作为编码）*/
    public static void addUFT8BOM(String filePath) throws IOException
    {
        RandomAccessFile randomFile = new RandomAccessFile(filePath, "rw");
        randomFile.write(new byte[] { (byte)0xEF, (byte)0xBB, (byte)0xBF });
        randomFile.close();
    }
}

package com.angejia.dw.build;

import java.io.*;

/*专门用来把build模板中的标志位替换成需要build的项目名*/
public class BuildHelper
{
    public static void main(String[] args) throws IOException
    {
        String proj = args[0];
        
        File ft = new File("../../build-template.xml");
        File f = new File("../../build.xml");
        if(!f.exists())
            f.createNewFile();
        
        BufferedReader br = new BufferedReader(new FileReader(ft));
        PrintWriter pr = new PrintWriter(f);
        
        String s;
        while((s = br.readLine()) != null)
            pr.println(s.replace("__proj__", proj));
        
        br.close();
        pr.close();
        
    }
}

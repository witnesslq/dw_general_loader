package com.angejia.dw.dw_general_loader;

public class DwGeneralLoadConf {

    // 项目部署目录
    private String dwGeneralLoadHome;
    public String getDwGeneralLoadHome() {
        return dwGeneralLoadHome;
    }
    public void setDwGeneralLoadHome(String dwGeneralLoadHome) {
        // 读取运行时的配置
        if (System.getProperty(dwGeneralLoadHome) != null) {
            this.dwGeneralLoadHome = System.getProperty(dwGeneralLoadHome);
        // 读取环境变量配置
        } else if(System.getenv(dwGeneralLoadHome) != null) {
            this.dwGeneralLoadHome = System.getenv(dwGeneralLoadHome);
        }
    }

    // hive home 目录
    private String dwHiveHome;
    public String getDwHiveHome() {
        return dwHiveHome;
    }
    public void setDwHiveHome(String dwHiveHome) {
        // 读取运行时的配置
        if (System.getProperty(dwHiveHome) != null) {
            this.dwHiveHome = System.getProperty(dwHiveHome);
        // 读取环境变量配置
        } else if(System.getenv(dwHiveHome) != null) {
            this.dwHiveHome = System.getenv(dwHiveHome);
        }
    }
}

package main;


public class Task {

    public static final int STATUS_EFFECTIVE = 2;

    public static final int TYPE_ETL = 1;
    public static final int TYPE_MYSQL = 2;
    public static final int TYPE_HIVE = 3;

    public static final int SOURCE_ETL_SLAVE = 1;
    public static final int SOURCE_DW_MASTER = 2;
    public static final int SOURCE_DW_IB = 3;
    public static final int SOURCE_HIVE = 4;
    public static final int SOURCE_HBASE = 5;
    public static final int SOURCE_HDFS = 6;

    public static final int TARGET_DW_STATS = 1;
    public static final int TARGET_DW_MASTER = 2;
    public static final int TARGET_DW_IB = 3;
    public static final int TARGET_HIVE = 4;
    public static final int TARGET_HBASE = 5;
    public static final int TARGET_HDFS = 6;

    public static final int MYSQL_STANDARD = 1;
    public static final int MYSQL_CUSTOM = 2;

    public static final int HIVE_STANDARD = 1;
    public static final int HIVE_CUSTOM = 2;

    private String moduleName;
    private Integer type;
    private String details;
    public String getModuleName() {
        return moduleName;
    }
    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }
    public Integer getType() {
        return type;
    }
    public void setType(Integer type) {
        this.type = type;
    }
    public String getDetails() {
        return details;
    }
    public void setDetails(String details) {
        this.details = details;
    }
    
}

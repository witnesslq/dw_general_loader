package main;

public class MysqlField {
    private String name;
    private String type;
    private Boolean extract;
    private Boolean cleanse;
    private Boolean load;
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public Boolean getExtract() {
        return extract;
    }
    public void setExtract(Boolean extract) {
        this.extract = extract;
    }
    public Boolean getCleanse() {
        return cleanse;
    }
    public void setCleanse(Boolean cleanse) {
        this.cleanse = cleanse;
    }
    public Boolean getLoad() {
        return load;
    }
    public void setLoad(Boolean load) {
        this.load = load;
    }
}

package cn.b2b.index.product.client;

import java.util.Date;

public class ProductUpdateBeanImpl implements IProductUpdateBean {
    private int productid;
    private String tradeid;
    private int industryid;
    private int memlevel;
    private int state;
    private Date insertdate;
    private Date outtime;
    private Date updatedate;
    private byte license;
    private int province;
    private int area;
    private int city;
   
    public int getProductid() {
        return productid;
    }
    public void setProductid(int productid) {
        this.productid = productid;
    }
    public String getTradeid() {
        return tradeid;
    }
    public void setTradeid(String tradeid) {
        this.tradeid = tradeid;
    }
    public int getIndustryid() {
        return industryid;
    }
    public void setIndustryid(int industryid) {
        this.industryid = industryid;
    }
    public int getMemlevel() {
        return memlevel;
    }
    public void setMemlevel(int memlevel) {
        this.memlevel = memlevel;
    }
    public int getState() {
        return state;
    }
    public void setState(int state) {
        this.state = state;
    }
    public Date getUpdatedate() {
        return updatedate;
    }
    public void setUpdatedate(Date updatedate) {
        this.updatedate = updatedate;
    }
    public byte getLicense() {
        return license;
    }
    public void setLicense(byte license) {
        this.license = license;
    }
    
    public int getProvince() {
        return province;
    }
    public void setProvince(int province) {
        this.province = province;
    }
    public int getArea() {
        return area;
    }
    public void setArea(int area) {
        this.area = area;
    }
    public int getCity() {
        return city;
    }
    public void setCity(int city) {
        this.city = city;
    }
    public Date getInsertdate() {
        return insertdate;
    }
    public void setInsertdate(Date insertdate) {
        this.insertdate = insertdate;
    }
    public Date getOuttime() {
        return outtime;
    }
    public void setOuttime(Date outtime) {
        this.outtime = outtime;
    }
}

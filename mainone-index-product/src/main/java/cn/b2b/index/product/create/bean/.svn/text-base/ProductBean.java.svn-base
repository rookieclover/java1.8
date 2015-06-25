package cn.b2b.index.product.create.bean;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.b2b.common.search.query.DataBean;
import cn.b2b.index.product.create.CityProvinceArea;
import cn.b2b.index.product.create.CompanyBean;

public class ProductBean {
    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    private DateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");    
    float boost = 1.0f;
    private int id;
    private int userid;
    private byte state;
    private byte auditstate;
    private String title;
    private String content;
    private int industryid;
    private String tradeid;
    private String keyword1;
    private String keyword2;
    private String keyword3;
    private int province;
    private int city;
    private String pic;
    private String price;
    private String brand;
    private Date insertdate;
    private Date updatedate;
    private Date outtime;
    private String url;
    private int datatype;
    private int memlevel;
    public int getNotfindproperties() {
		return notfindproperties;
	}

	public void setNotfindproperties(int notfindproperties) {
		this.notfindproperties = notfindproperties;
	}

	private int license;
    private boolean haspic;
    private int notfindproperties;
    
    public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}
	
	private String unit;
	private String spec;
	
	public String getSpec() {
		return spec;
	}

	public void setSpec(String spec) {
		this.spec = spec;
	}

	public String getMincount() {
		return mincount;
	}

	public void setMincount(String mincount) {
		this.mincount = mincount;
	}

	private String mincount;

    // / 公司名称
    private String companyName;
    // / 公司地址
    private String companyUrl;
    // / 公司Id
    private int companyId;
    // / 公司地址
    private String companyAddress;
    // / 公司规模
    private int companyScale;

    /**
     * 企业执照认证
     */
    private int buslincese =0;
    
    public float getBoost() {
        return boost;
    }

    public void setBoost(float boost) {
        this.boost = boost;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public byte getState() {
        return state;
    }

    public void setState(byte state) {
        this.state = state;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getIndustryid() {
        return industryid;
    }

    public void setIndustryid(int industryid) {
        this.industryid = industryid;
    }

    public String getTradeid() {
        return tradeid;
    }

    public void setTradeid(String tradeid) {
        this.tradeid = tradeid;
    }

    public String getKeyword1() {
        return keyword1;
    }

    public void setKeyword1(String keyword1) {
        this.keyword1 = keyword1;
    }

    public String getKeyword2() {
        return keyword2;
    }

    public void setKeyword2(String keyword2) {
        this.keyword2 = keyword2;
    }

    public String getKeyword3() {
        return keyword3;
    }

    public void setKeyword3(String keyword3) {
        this.keyword3 = keyword3;
    }

    public int getProvince() {
        return province;
    }

    public void setProvince(int province) {
        this.province = province;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public String getPic() {
        return pic;
    }

    public void setPic(String pic) {
        this.pic = pic;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public Date getInsertdate() {
        return insertdate;
    }

    public void setInsertdate(Date insertdate) {
        this.insertdate = insertdate;
    }

    public Date getUpdatedate() {
        return updatedate;
    }

    public void setUpdatedate(Date updatedate) {
        this.updatedate = updatedate;
    }

    public Date getOuttime() {
        return outtime;
    }

    public void setOuttime(Date outtime) {
        this.outtime = outtime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getDatatype() {
        return datatype;
    }

    public void setDatatype(int datatype) {
        this.datatype = datatype;
    }

    public int getMemlevel() {
        return memlevel;
    }

    public void setMemlevel(int memlevel) {
        this.memlevel = memlevel;
    }

    public int getLicense() {
        return license;
    }

    public void setLicense(int license) {
        this.license = license;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getCompanyUrl() {
        return companyUrl;
    }

    public void setCompanyUrl(String companyUrl) {
        this.companyUrl = companyUrl;
    }

    public int getCompanyId() {
        return companyId;
    }

    public void setCompanyId(int companyId) {
        this.companyId = companyId;
    }

    public String getCompanyAddress() {
        return companyAddress;
    }

    public void setCompanyAddress(String companyAddress) {
        this.companyAddress = companyAddress;
    }

    public int getCompanyScale() {
        return companyScale;
    }

    public void setCompanyScale(int companyScale) {
        this.companyScale = companyScale;
    }

    public DataBean toDataBean() {
        DataBean product = new DataBean();

        float boost = 1.0f;
        product.add("docnum", id + "");
        product.add("userid", userid + "");
        if (state == 1 /*&& auditstate == 0*/) {
        	product.add("state", "1");
        } else {
        	product.add("state", "0");
        }
        product.add("title", title);
        int titlelen = getLen(title);
        if (titlelen <= 8) {
            boost -= 0.2f;
        }
        if (titlelen > 30) {
            boost += 0.2f;
        }
        product.add("anchor", title);
        product.add("titlelen", titlelen + "");
        product.add("content", content);
        int contentlen = getLen(content);
        product.add("conlen", contentlen + "");

        if (contentlen <= 100) {
            boost -= 0.1f;
        }
        if (contentlen > 600) {
            boost += 0.1f;
        }
        product.add("tradeid", tradeid);
        product.add("industryid", industryid + "");
        product.add("keyword", keyword1 + " " + keyword2 + " " + keyword3);

        product.add("city", city + "");
        if (province != getProvinceByCity(city)) {
            province = getProvinceByCity(city);
        }
        product.add("province", province + "");
        int area = getAreaByCity(city);
        product.add("area", area + "");
        product.add("pic", pic);
        if (haspic) {
            product.add("haspic", "1");
            boost += 2.0f;
        } else {
            product.add("haspic", "0");
        }
        product.add("price", price + "");

        product.add("brand", brand);
        if (insertdate != null) {
            product.add("idate", insertdate.getTime() + "");
            boost += 0.1f;
        } else {
            product.add("idate", "0");
        }
        if (insertdate != null) {
            product.add("insertdate", dateFormat.format(insertdate) + "");
            product.add("idate", insertdate.getTime() + "");
        } else {
            product.add("insertdate", "19700101 08:01:01");
            product.add("idate", "0");
        }
        if (updatedate != null) {
            product.add("updatedate", dateFormat.format(updatedate) + "");
            product.add("udate", updatedate.getTime() + "");
            product.add("utime", getUpdateDay(updatedate) + "");
        } else {
            product.add("updatedate", "19700101 08:01:01");
            product.add("udate", "0");
            product.add("utime", "0");
        }
        
        if (outtime != null) {
            product.add("outtime", dateFormat.format(outtime) + "");
            product.add("otime", outtime.getTime() + "");
        } else {
            product.add("outtime", "19700101 08:01:01");
            product.add("otime", "0");
        }
        
        product.add("url", url);
        product.add("datatype", datatype + "");
        product.add("memlevel", memlevel + "");
        product.add("license", license + "");
        product.add("cname", companyName);
        product.add("caddress", companyAddress);
        product.add("size", companyScale + "");
        product.add("curl", companyUrl);
        product.add("cid", companyId + "");
        product.add("rank", boost + "");
        product.add("buslincese", buslincese + "");
        
        product.add("spec", spec + "");
        product.add("unit", unit + "");
        product.add("mincount", mincount + "");
        product.setBoost(boost);
        return product;
    }

    private long getUpdateDay(Date updatedate2) {

        String daystr = dayFormat.format(updatedate2);
        Date date = updatedate2;
        try {
            date = dayFormat.parse(daystr);
        } catch (ParseException e) {
            
        }
        return date.getTime();
    }

    private int getLen(String string) {
        if (string != null) {
            byte[] bytes = string.getBytes();
            return bytes.length;
            
        } else {
            return 0;
        }
    }

    private int getProvinceByCity(int city) {
        if (CityProvinceArea.CITY_PROVINCE_MAP.get(city) != null) {
            int province = CityProvinceArea.CITY_PROVINCE_MAP.get(city);
            return province;
        }
        return -1;
    }

    private int getAreaByCity(int city) {
        if (CityProvinceArea.CITY_PROVINCE_MAP.get(city) != null) {
            int province = CityProvinceArea.CITY_PROVINCE_MAP.get(city);
            if (CityProvinceArea.PROVINCE_AREA_MAP.get(province) != null) {
                int area = CityProvinceArea.PROVINCE_AREA_MAP.get(province);
                return area;
            }
        }
        return -1;
    }

    public void setCompany(CompanyBean companyBean) {
        try {
            if (companyBean != null) {
                setCompanyName(companyBean.getCompanyName());
                setCompanyUrl(companyBean.getCompanyUrl());
                setCompanyId(companyBean.getCompanyId());
                setCompanyAddress(companyBean.getCompanyAddress());
                setCompanyScale(companyBean.getCompanyScale());
                setMemlevel(companyBean.getMemberLevel());

            } else {
                setCompanyName("");
                setCompanyUrl("");
                setCompanyId(0);
                setCompanyAddress("");
                setCompanyScale(0);
                setMemlevel(-1);
            }
        } catch (Exception e) {
            setCompanyName("");
            setCompanyUrl("");
            setCompanyId(0);
            setCompanyAddress("");
            setCompanyScale(0);
            setMemlevel(-1);
        }
    }

	public byte getAuditstate() {
		return auditstate;
	}

	public void setAuditstate(byte auditstate) {
		this.auditstate = auditstate;
	}

	public boolean isHaspic() {
		return haspic;
	}

	public void setHaspic(boolean haspic) {
		this.haspic = haspic;
	}

	public int getBuslincese() {
		return buslincese;
	}

	public void setBuslincese(int buslincese) {
		this.buslincese = buslincese;
	} 
}

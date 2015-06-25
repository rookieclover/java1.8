package cn.b2b.index.product.create;

import java.io.IOException;

import cn.b2b.common.search.CreateConfig;

public class ProductCreateConfig extends CreateConfig {
    
    private String mssqlDriver;
    private String companyUrl;
    private String companyUsername;
    private String companyPassword;
    
    private String mysqlDriver;
    private String[] mysqlUrls;
    private String[] mysqlUsernames;
    private String[] mysqlPasswords;
    private String tradeUrl;
    private String tradeUsername;
    private String tradePassword;
    
    private String certiUrl;
    private String certiUsername;
    private String certiPassword;
    
    private String[] databases;
    
    private String cateInduFile;
    private String maxIdFilePath;
    
    private String[] urlDBs;
    
    private int startNum;
    private int limitNum;
    private String mongoDbHost;
    private int mongoDbPort;
    private String mongoDbName;
    private String mongoDbCollection;
    
    private int userIdBegin;
    public int getMaxId() {
		return maxId;
	}

	public void setMaxId(int maxId) {
		this.maxId = maxId;
	}

	private int userIdEnd;
    private int maxId;
    
    public int getStartNum() {
		return startNum;
	}

	public void setStartNum(int startNum) {
		this.startNum = startNum;
	}

	public int getLimitNum() {
		return limitNum;
	}

	public void setLimitNum(int limitNum) {
		this.limitNum = limitNum;
	}

	public String getMongoDbHost() {
		return mongoDbHost;
	}

	public void setMongoDbHost(String mongoDbHost) {
		this.mongoDbHost = mongoDbHost;
	}

	public int getMongoDbPort() {
		return mongoDbPort;
	}

	public void setMongoDbPort(int mongoDbPort) {
		this.mongoDbPort = mongoDbPort;
	}

	public String getMongoDbName() {
		return mongoDbName;
	}

	public void setMongoDbName(String mongoDbName) {
		this.mongoDbName = mongoDbName;
	}

	public String getMongoDbCollection() {
		return mongoDbCollection;
	}

	public void setMongoDbCollection(String mongoDbCollection) {
		this.mongoDbCollection = mongoDbCollection;
	}

	public void loadConfig(String configFile) throws IOException {
        super.loadConfig(configFile);
        
        mongoDbHost= prop.getProperty(ProductCreateConstants.MONGO_DB_HOST, "");
        mongoDbPort=Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_PORT, ""));
        mongoDbName=prop.getProperty(ProductCreateConstants.MONGO_DB_NAME, "");
        mongoDbCollection=prop.getProperty(ProductCreateConstants.MONGO_DB_COLLECTION, "");
        startNum=Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_START_NUM, "0"));
        limitNum=Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_LIMIT_NUM, "100000"));
        
        userIdBegin=Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_USERID_BEGIN,"0"));
        userIdEnd=Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_USERID_END,"1"));
        
         
        mssqlDriver = prop.getProperty(ProductCreateConstants.MSSQL_DRIVER, ProductCreateConstants.DEFAULT_MSSQL_DRIVER);
        companyUrl = prop.getProperty(ProductCreateConstants.COMPANY_URL, ProductCreateConstants.DEFAULT_COMPANY_URL);
        companyUsername = prop.getProperty(ProductCreateConstants.COMPANY_USERNAME, ProductCreateConstants.DEFAULT_COMPANY_USERNAME);
        companyPassword = prop.getProperty(ProductCreateConstants.COMPANY_PASSWORD, ProductCreateConstants.DEFAULT_COMPANY_PASSWORD);
        
        mysqlDriver = prop.getProperty(ProductCreateConstants.MYSQL_DRIVER, ProductCreateConstants.DEFAULT_MYSQL_DRIVER);
        
        tradeUrl = prop.getProperty(ProductCreateConstants.MYSQL_TRADE_URL, ProductCreateConstants.DEFAULT_MYSQL_TRADE_URL);
        tradeUsername = prop.getProperty(ProductCreateConstants.MYSQL_TRADE_USERNAME, ProductCreateConstants.DEFAULT_MYSQL_TRADE_USERNAME);
        tradePassword = prop.getProperty(ProductCreateConstants.MYSQL_TRADE_PASSWORD, ProductCreateConstants.DEFAULT_MYSQL_TRADE_PASSWORD);
        
        certiUrl = prop.getProperty(ProductCreateConstants.MYSQL_CERTI_URL, ProductCreateConstants.DEFAULT_MYSQL_CERTI_URL);
        certiUsername = prop.getProperty(ProductCreateConstants.MYSQL_CERTI_USERNAME, ProductCreateConstants.DEFAULT_MYSQL_CERTI_USERNAME);
        certiPassword = prop.getProperty(ProductCreateConstants.MYSQL_CERTI_PASSWORD, ProductCreateConstants.DEFAULT_MYSQL_CERTI_PASSWORD);
        
        String urlDB = prop.getProperty(ProductCreateConstants.PRODUCT_SQL_URL_DB,"");
        urlDBs = urlDB.split(",");
        mysqlUrls = new String[urlDBs.length];
        mysqlUsernames = new String[urlDBs.length];
        mysqlPasswords = new String[urlDBs.length];
        databases = new String[urlDBs.length];
        
        //mysqlUrl = prop.getProperty(ProductCreateConstants.MYSQL_URL, ProductCreateConstants.DEFAULT_MYSQL_URL);
        //mysqlUsername = prop.getProperty(ProductCreateConstants.MYSQL_USERNAME, ProductCreateConstants.DEFAULT_MYSQL_USERNAME);
        //mysqlPassword = prop.getProperty(ProductCreateConstants.MYSQL_PASSWORD, ProductCreateConstants.DEFAULT_MYSQL_PASSWORD);
        
        for (int i = 0; i < urlDBs.length; i++) {
            String mysqlUrl = prop.getProperty(urlDBs[i] + "_" + ProductCreateConstants.MYSQL_URL, ProductCreateConstants.DEFAULT_MYSQL_URL);
            String mysqlUsername = prop.getProperty(urlDBs[i] + "_" + ProductCreateConstants.MYSQL_USERNAME, ProductCreateConstants.DEFAULT_MYSQL_USERNAME);
            String mysqlPassword = prop.getProperty(urlDBs[i] + "_" + ProductCreateConstants.MYSQL_PASSWORD, ProductCreateConstants.DEFAULT_MYSQL_PASSWORD);
            String database = prop.getProperty(urlDBs[i] + "_" + ProductCreateConstants.DATABASE_NAME);
            mysqlUrls[i] = mysqlUrl;
            mysqlUsernames[i] = mysqlUsername;
            mysqlPasswords[i] = mysqlPassword;
            databases[i] = database;
        }
        cateInduFile = prop.getProperty(ProductCreateConstants.TRADE_INDUSTRY_FILE_NAME);
        maxIdFilePath = prop.getProperty(ProductCreateConstants.MAX_ID_FILE_PATH);
        
    }

    public String getMssqlDriver() {
        return mssqlDriver;
    }

    public String getCompanyUrl() {
        return companyUrl;
    }

    public String getCompanyUsername() {
        return companyUsername;
    }

    public String getCompanyPassword() {
        return companyPassword;
    }

    public String getMysqlDriver() {
        return mysqlDriver;
    }

    public int getUserIdBegin() {
		return userIdBegin;
	}

	public void setUserIdBegin(int userIdBegin) {
		this.userIdBegin = userIdBegin;
	}

	public int getUserIdEnd() {
		return userIdEnd;
	}

	public void setUserIdEnd(int userIdEnd) {
		this.userIdEnd = userIdEnd;
	}

	public String[] getMysqlUrls() {
        return mysqlUrls;
    }

    public String[] getMysqlUsernames() {
        return mysqlUsernames;
    }

    public String[] getMysqlPasswords() {
        return mysqlPasswords;
    }

    public String getTradeUrl() {
        return tradeUrl;
    }

    public String getTradeUsername() {
        return tradeUsername;
    }

    public String getTradePassword() {
        return tradePassword;
    }


    public String getCateInduFile() {
        return cateInduFile;
    }

    public String[] getDatabases() {
        return databases;
    }

    public String getCertiUrl() {
        return certiUrl;
    }

    public String getCertiUsername() {
        return certiUsername;
    }

    public String getCertiPassword() {
        return certiPassword;
    }
    
    public String getMaxIdFilePath() {
    	return maxIdFilePath;
    }
    
    public String getMaxIdFilePath(int bg,int ed) {
    	return maxIdFilePath +"/MONGO_MAXID_"+bg+"_"+ed+".conf";
    }

    public String[] getUrlDBs() {
        return urlDBs;
    }
    
}

package cn.b2b.index.product.create;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.b2b.index.product.create.bean.ProductBean;
import cn.b2b.index.product.dbmanager.MSSQLConnectionImpl;
import cn.b2b.index.product.dbmanager.MySQLConnectionImpl;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class ProductDataFromDB {
	private static final Logger LOG = Logger.getLogger("create");

	private static final String COMPANY_SQL = "SELECT c.[UserID], [CompanyID], "
			+ " [CompanyName], [MemberLevel], "
			+ " [CompanySize], [CompanyArtificial], "
			+ " [CompanyWeb],  uc.CompanyAddress  ,u.state & 1 auditstate "
			+ " FROM [NWME_Company_Index] c , [NWME_Company_Exp1] ce, NWME_UserContact uc,nwme_user u left join [NWME_Company_URL] cu on"
			+ " u.id=cu.userid where c.userid=ce.userid and [CompanyID]>0 and c.userid=uc.userid and c.userid=u.id and c.userid between ? and ? "
			+ " order by c.userid";

	private static final String COMPANY_USER_ID_MIN_SQL = "SELECT MIN(UserID) from [NWME_Company_Index] where [CompanyID]>0";
	private static final String COMPANY_USER_ID_MAX_SQL = "SELECT MAX(UserID) from [NWME_Company_Index] where [CompanyID]>0";

	private static final String CERTIPASS_SQL = "SELECT userid from wme_certipass WHERE perioddate>now()";

	private static final String TRADE_SQL = "select elementid, parentid from trade_category";

	private static final String LINCESE_SQL = "select userid uid from [nwme_businesslicence] where auditstate=0 and outTime >= getdate()";

	// private static final String MEMLEVEL_SQL =
	// "select userid, MemberLevel from NWME_Company_Index where MemberLevel > 0";

	// elementid 行业ID
	// parentid 父级ID
	private static final String PRODUCT_ID_MIN_SQL = "SELECT MIN(id) from trade";
	private static final String PRODUCT_ID_MAX_SQL = "SELECT MAX(id) from trade";
	private Connection companyConn;
	private Connection tradeConn;
	private Connection certiconn;
	private Map<Integer, Integer> certiMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> linceseMap = new HashMap<Integer, Integer>();
	private Map<Integer, CompanyBean> companyMap = new HashMap<Integer, CompanyBean>();
	private static final Map<Integer, Integer> tradeMap = new HashMap<Integer, Integer>();

	public void init(ProductCreateConfig conf) throws SQLException {
		try {
			companyConn = new MSSQLConnectionImpl(conf.getCompanyUrl(),
					conf.getCompanyUsername(), conf.getCompanyPassword())
					.openConnection();
			LOG.info("MSCONN:" + companyConn);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
		try {
			tradeConn = new MySQLConnectionImpl(conf.getTradeUrl(),
					conf.getTradeUsername(), conf.getTradePassword())
					.openConnection();
			LOG.info("TRADECONN:" + tradeConn);
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
		try {
			certiconn = new MySQLConnectionImpl(conf.getCertiUrl(),
					conf.getCertiUsername(), conf.getCertiPassword())
					.openConnection();
			LOG.info("CERTICONN:" + certiconn);
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}

		clear();
		initCertiPass();
		initLincese();
		initCompanyMap();
		initTrade();
	}

	private void clear() {
		certiMap.clear();
		linceseMap.clear();
		companyMap.clear();
		tradeMap.clear();
	}

	private void initCertiPass() throws SQLException {
		PreparedStatement certps = certiconn.prepareStatement(CERTIPASS_SQL);
		ResultSet rs = certps.executeQuery();
		while (rs.next()) {
			certiMap.put(rs.getInt(1), 1);
		}
		rs.close();
		certps.close();
		certiconn.close();
	}

	private void initLincese() throws SQLException {
		PreparedStatement lincese = companyConn.prepareStatement(LINCESE_SQL);
		ResultSet rs = lincese.executeQuery();
		while (rs.next()) {
			linceseMap.put(rs.getInt(1), 1);
		}
		rs.close();
		lincese.close();
	}

	private void initCompanyMap() throws SQLException {
		int maxid = 0;
		int minid = 0;
		Statement st = companyConn.createStatement();
		ResultSet mrs = st.executeQuery(COMPANY_USER_ID_MIN_SQL);
		if (mrs.next()) {
			minid = mrs.getInt(1);
		}
		mrs.close();
		st.close();
		st = companyConn.createStatement();
		mrs = st.executeQuery(COMPANY_USER_ID_MAX_SQL);
		if (mrs.next()) {
			maxid = mrs.getInt(1);
		}
		mrs.close();
		st.close();
		PreparedStatement companyps = companyConn.prepareStatement(COMPANY_SQL);
		while (minid < maxid) {
			companyps.setInt(1, minid);
			minid += 10000;
			companyps.setInt(2, minid);
			minid++;
			ResultSet rs = companyps.executeQuery();
			while (rs.next()) {
				CompanyBean companyBean = new CompanyBean();
				companyBean.setUrserId(rs.getInt("UserID"));
				companyBean.setCompanyId(rs.getInt("CompanyID"));
				companyBean.setCompanyName(rs.getString("CompanyName"));
				companyBean.setCompanyAddress(rs.getString("CompanyAddress"));
				companyBean.setCompanyScale(rs.getInt("CompanySize"));
				companyBean.setCompanyUrl(rs.getString("CompanyWeb"));
				companyBean.setAuditState(rs.getInt("auditstate"));
				int memlevel = rs.getInt("MemberLevel");
				if (memlevel > 1000) {
					memlevel -= 1001;
					companyBean.setMemberLevel(memlevel);
				}
				companyMap.put(rs.getInt("UserID"), companyBean);
			}
			rs.close();
		}
		companyps.close();
		companyConn.close();
	}

	private void initTrade() throws SQLException {
		Statement mst = tradeConn.createStatement();
		ResultSet rs = mst.executeQuery(TRADE_SQL);
		while (rs.next()) {
			int eid = rs.getInt("elementid");
			int pid = rs.getInt("parentid");
			if (pid != 0) {
				tradeMap.put(eid, pid);
			}
		}
		rs.close();
		mst.close();
		tradeConn.close();
	}

	public ProductBean getProductData(ResultSet rs) throws SQLException {
		if (rs == null) {
			return null;
		}
		ProductBean product = new ProductBean();
		product.setId(rs.getInt("id"));
		int userid = rs.getInt("userid");
		product.setUserid(userid);
		product.setState((byte) rs.getInt("state"));
		if (companyMap.get(userid) != null
				&& companyMap.get(userid).getAuditState() == 0) {
			product.setState((byte) 0);
		}
		product.setAuditstate(rs.getByte("auditstate"));

		String title = rs.getString("title");
		if (title != null) {
			product.setTitle(rs.getString("title"));

		} else {
			product.setTitle("");
		}

		String content = rs.getString("content");
		if (content != null) {
			if (content.length() > 500) {
				product.setContent(content.substring(0, 500));
			} else {
				product.setContent(content);
			}
		} else {
			product.setContent("");
		}
		product.setIndustryid(rs.getInt("industryid"));
		String tradeid = this.getTrade(rs.getInt("tradeid"));
		product.setTradeid(tradeid);
		product.setKeyword1(rs.getString("keyword1"));
		product.setKeyword2(rs.getString("keyword2"));
		product.setKeyword3(rs.getString("keyword3"));
		product.setProvince(rs.getInt("province"));
		product.setCity(rs.getInt("city"));

		if (rs.getString("pic") != null
				&& rs.getString("pic").trim().length() > 0) {
			product.setHaspic(true);
			product.setPic("http://files.b2b.cn/product/ProductImages/"
					+ rs.getString("pic"));
		} else {
			product.setPic("http://img.b2b.cn/default/20120217/images/nop2.png");
			product.setHaspic(false);
		}

		product.setPrice(rs.getString("price"));
		String property = rs.getString("property");
		if (property != null) {
			String brand = getBrand(property);
			if (brand != null) {
				product.setBrand(brand);
			} else {
				product.setBrand("");
			}
		} else {
			product.setBrand("");
		}
		/*rookie_guanrong add 2015-05-21 start*/
		//product.setSpec(this.getProperty(json_p_property, "P3"));

		//product.setUnit(this.getProperty(json_t_property, "T35"));
		//product.setMincount(this.getProperty(json_t_property, "T1"));
		if (property != null)
		{
			String spec = getItemByXml(property,"P3");
			if (spec != null) 
			{
				product.setSpec(spec);
			}
			else
			{
				product.setSpec("");
			}
		}
		else
		{
			product.setSpec("");
		}
		String tradeproperty = rs.getString("tradeproperty");
		if (tradeproperty != null)
		{
			String unit = getItemByXml(tradeproperty,"T35");
			if (unit != null) 
			{
				product.setUnit(unit);
			}
			else
			{
				product.setUnit("");
			}
		}
		else
		{
			product.setUnit("");
		}
		if (tradeproperty != null)
		{
			String mincount = getItemByXml(tradeproperty,"T1");
			if (mincount != null) 
			{
				product.setMincount(mincount);
			}
			else
			{
				product.setMincount("");
			}
		}
		else
		{
			product.setMincount("");
		}
		/*end*/
		product.setInsertdate(rs.getDate("insertdate"));
		product.setUpdatedate(rs.getDate("updatedate"));
		product.setOuttime(rs.getDate("outtime"));
		product.setUrl(rs.getString("url"));
		if (certiMap.get(userid) != null) {
			product.setLicense(1);
		} else {
			product.setLicense(0);
		}
		if (companyMap.get(userid) != null) {
			product.setCompany(companyMap.get(userid));
		} else {
			product.setCompany(null);
		}
		product.setDatatype(2);
		product.setBuslincese(linceseMap.get(userid) == null ? 0 : 1);
		return product;
	}
	
	public ProductBean getProductData(DBObject obj) {
		if (obj == null) {
			return null;
		}
		ProductBean product = new ProductBean();
		product.setId(toInt(obj.get("_id")));
		int userid = toInt(obj.get("uid"));
		product.setUserid(userid);
		product.setState((byte) toInt(obj.get("ss")));
		if (companyMap.get(userid) != null
				&& companyMap.get(userid).getAuditState() == 0) {
			product.setState((byte) 0);
		}
		product.setAuditstate((byte) toInt(obj.get("wait")));

		String title = String.valueOf(obj.get("titl"));
		if (title != null) {
			product.setTitle(title);

		} else {
			product.setTitle("");
		}

		String content = toString(obj.get("con"));
		if (content != null) {
			if (content.length() > 500) {
				product.setContent(content.substring(0, 500));
			} else {
				product.setContent(content);
			}
		} else {
			product.setContent("");
		}
		product.setIndustryid(toInt(obj.get("inid")));
		String tradeid = this.getTrade(toInt(obj.get("tr1")));
		product.setTradeid(tradeid);
		product.setKeyword1(toString(obj.get("k1")));
		product.setKeyword2(toString(obj.get("k2")));
		product.setKeyword3(toString(obj.get("k3")));
		product.setProvince(toInt(obj.get("prov")));
		product.setCity(toInt(obj.get("city")));

		String pic = toString(obj.get("pic1"));
		if (pic != null && pic.trim().length() > 0) {
			product.setHaspic(true);
			product.setPic("http://files.b2b.cn/product/ProductImages/" + pic);
		} else {
			product.setPic("http://img.b2b.cn/default/20120217/images/nop2.png");
			product.setHaspic(false);
		}
		product.setPrice(toString(obj.get("pric")));

		String p_property = toString(obj.get("pro"));
		String t_property = toString(obj.get("tpro"));

		DBObject json_p_property = null;

		try {
			json_p_property = (DBObject) JSON.parse(p_property);
		} catch (Exception ex) {
			product.setNotfindproperties(1);
			ex.printStackTrace();
		}
		DBObject json_t_property = null;
		try {
			json_t_property = (DBObject) JSON.parse(t_property);
		} catch (Exception ex) {
			product.setNotfindproperties(1);
			ex.printStackTrace();
		}

		product.setBrand(this.getProperty(json_p_property, "P1"));
		product.setSpec(this.getProperty(json_p_property, "P3"));

		product.setUnit(this.getProperty(json_t_property, "T35"));
		product.setMincount(this.getProperty(json_t_property, "T1"));

		product.setInsertdate(toDatetime(toInt(obj.get("atim"))));
		product.setUpdatedate(toDatetime(toInt(obj.get("utim"))));
		product.setOuttime(toDatetime(toInt(obj.get("utim"))));
		product.setUrl("http://detail.b2b.cn/product/" + product.getId()
				+ ".html");
		if (certiMap.get(userid) != null) {
			product.setLicense(1);
		} else {
			product.setLicense(0);
		}
		if (companyMap.get(userid) != null) {
			product.setCompany(companyMap.get(userid));
		} else {
			product.setCompany(null);
		}
		product.setDatatype(2);
		product.setBuslincese(linceseMap.get(userid) == null ? 0 : 1);
		return product;
	}

	private String getTrade(int companyVocation1) {
		String result = "";
		result = companyVocation1 + "";
		if (tradeMap.get(companyVocation1) != null) {
			int parent = tradeMap.get(companyVocation1);
			if (parent > 0) {
				result += " " + getTrade(parent);
			}
		}
		return result;
	}

	public int getMinProductID(Connection conn) throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(PRODUCT_ID_MIN_SQL);
		int result = -1;
		if (rs.next()) {
			result = rs.getInt(1);
		}
		rs.close();
		st.close();
		return result;
	}

	public int getMaxProductID(Connection conn) throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(PRODUCT_ID_MAX_SQL);
		int result = -1;
		if (rs.next()) {
			result = rs.getInt(1);
		}
		rs.close();
		st.close();
		return result;

	}

	private String getBrand(String xml) {
		Document doc;
		try {
			doc = DocumentHelper.parseText(xml);

			Element rootNode = doc.getRootElement();
			Iterator iter = rootNode.nodeIterator();
			while (iter.hasNext()) {
				Element recordEle = (Element) iter.next();
				String title = recordEle.getName();
				if (title.equals("P1")) {
					return recordEle.getText();
				}
				return "";
			}
		} catch (DocumentException e) {
			// e.printStackTrace();
		}
		return "";
	}
	private String getItemByXml(String xml,String nodeName) {
		Document doc;
		try {
			doc = DocumentHelper.parseText(xml);

			Element rootNode = doc.getRootElement();
			Iterator iter = rootNode.nodeIterator();
			while (iter.hasNext()) {
				Element recordEle = (Element) iter.next();
				String title = recordEle.getName();
				if (title.equals(nodeName)) {
					return recordEle.getText();
				}
				return "";
			}
		} catch (DocumentException e) {
			// e.printStackTrace();
		}
		return "";
	}
	private String getProperty(DBObject object, String key) {
		if (object == null)
			return "";
		return toString(object.get(key));// .get("P1"));
	}

	private int toInt(Object obj) {
		if (obj == null)
			return -1;
		return Integer.parseInt(String.valueOf(obj));
	}

	private String toString(Object obj) {
		if (obj == null)
			return "";
		return String.valueOf(obj);
	}

	private Date toDatetime(int unixtimestamp) {
		/*
		 * java.util.Calendar cd = Calendar.getInstance();
		 * cd.setTimeInMillis(unixtimestamp*1000*1000); return cd.getTime();
		 */
		long l = (long) unixtimestamp * 1000;
		return new Date(l);

	}

}

package cn.b2b.index.product.create;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import cn.b2b.common.search.query.rule.QueryRuleManager;
import cn.b2b.common.search.score.MainOneSimilarity;
import cn.b2b.common.search.transfer.MovePath;
import cn.b2b.common.search.util.DistributionIndex;
import cn.b2b.common.search.util.FileTools;
import cn.b2b.common.search.util.Stopwords;
import cn.b2b.create.index.create.IndexWriter;
import cn.b2b.index.product.create.bean.ProductBean;
import cn.b2b.index.product.dbmanager.MySQLConnectionImpl;

public class IndexProductIntimeFromDB {

	private static final Logger LOG = Logger.getLogger("create");
	private static final String PRODUCT_INDEX_SEG_NAME = "product-index-intime";
	private static boolean breakindex = false;
	private ProductCreateConfig conf;
	private static int BATCH_STEP = 10000;
	private ProductDataFromDB productDataFromDB = null;
	private Map<String, String> idsMap = new HashMap<String, String>();
	private DistributionIndex di = null;
	private Calendar last_init_time = null;
	
	private static final String PRODUCT_SQL = "select t.id as id, t.`userid` as userid, state&POWER(2,1)=POWER(2,1) as state,"
			// /*1 上架 0 下架*/
			+ " state&POWER(2,9)=POWER(2,9) as auditstate," // /*是否通过审核 *0 通过审核
															// 1 未通过审核*/
			+ " d.title as title, d.`content` as content, t.`industryid` as industryid,"
			+ " d.`tradeid1` as tradeid, d.`keyword1` as keyword1, d.`keyword2` as keyword2, d.`keyword3` as keyword3,"
			+ " d.`province` as province, d.`city` as city, d.`picture1` as pic,"
			+ " d.`price` as price, d.`property` as property,d.`tradeproperty` as tradeproperty,"
			+ // 产品属性XML,/* P1 品牌*/
			" FROM_UNIXTIME(insertdate) as insertdate, FROM_UNIXTIME(updatedate) as updatedate, FROM_UNIXTIME(outtime) as outtime,"
			+ " concat('http://detail.b2b.cn/product/',t.id,'.html') as url "
			+ " from trade t,`tradedetail` d"
			+ " where t.id=d.id and t.id between ? and ? " + " order by t.id";

	public IndexProductIntimeFromDB(String configpath) {
		try {
			last_init_time =Calendar.getInstance();
			conf = new ProductCreateConfig();
			conf.loadConfig(configpath);
			Stopwords.loadStopwords("stopwords.txt");
			Stopwords.loadSymbol("symbol.txt");
			Stopwords.loadHighFreq("highfreq.txt");
			idsMap = new HashMap<String, String>();
			Stopwords.loadClassPath();
			productDataFromDB = new ProductDataFromDB();
			productDataFromDB.init(conf);
			QueryRuleManager.getRulePlugin(conf.getQueryRuleClass());

		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/**
	 * 重新加载数据到内存 每天重新加载一次
	 * @throws SQLException 
	 */
	public void initFromDB() throws SQLException
	{
		Calendar cal =Calendar.getInstance();
		if(last_init_time.get(Calendar.DATE) != cal.get(Calendar.DATE))
		{
			productDataFromDB.init(conf);
			last_init_time = Calendar.getInstance();
			LOG.info("Reload productDataFromDB init...");
		}
	}

	public void build(boolean isDistributionIndex) {
		String[] dbURL = conf.getMysqlUrls();
		String[] usernames = conf.getMysqlUsernames();
		String[] passwords = conf.getMysqlPasswords();
		String[] databasess = conf.getDatabases();
		while (true) {
			try {
				
				this.initFromDB();
				
				String segmentName = conf.getSegmentName();
				if (segmentName == null) {
					segmentName = PRODUCT_INDEX_SEG_NAME;
				}
				
				File segment = FileTools
						.createFile(conf.getPath(), segmentName);
				
				//File segment  = null;
				IndexWriter iw = new IndexWriter(conf, segment, false);
				iw.setSimilarity(new MainOneSimilarity());
				long time1 = System.currentTimeMillis();
				int count = 0;
				boolean builder = false;
				for (int k = 0; k < dbURL.length; k++) {
					String[] databases = databasess[k].split(",");
					builder = checkMaxForBuild(dbURL[k], usernames[k],
							passwords[k], databases);
					if (builder)
						break;
				}
				if (builder) {
					for (int k = 0; k < dbURL.length; k++) {
						String[] databases = databasess[k].split(",");
						// 开始建索引
						for (int i = 0; i < databases.length; i++) {
							Connection conn = new MySQLConnectionImpl(dbURL[k]
									+ databases[i], usernames[k], passwords[k])
									.openConnection();
							int minID = this.initMinDocid(databases[i]);
							int maxID = productDataFromDB.getMaxProductID(conn);
							idsMap.put(databases[i], minID + "," + maxID);

							PreparedStatement productps = conn
									.prepareStatement(PRODUCT_SQL);

							while (minID < maxID) {
								ResultSet productrs = null;
								try {
									LOG.info("current minID:" + minID);

									productps.setInt(1, minID);
									productps.setInt(2, minID + BATCH_STEP);
									minID = minID + BATCH_STEP + 1;

									productrs = productps.executeQuery();
									while (productrs.next()) {

										ProductBean product = productDataFromDB
												.getProductData(productrs);
										if (product == null) {
											continue;
										}
										if (count % 10000 == 0) {
											LOG.info("Index Product by "
													+ count);
										}

										iw.addDocument(product.toDataBean());
										count++;
									}

								} catch (Exception e) {
									LOG.error("build:", e);
									e.printStackTrace();
								}
							}
							productps.close();
							conn.close();
						}
					}

					long totalEnd = System.currentTimeMillis();
					time1 = totalEnd;
					LOG.info("total time:" + (totalEnd - time1) + "\t count:"
							+ count);
					iw.close();
					
					if(di==null)
					{
						di = new DistributionIndex(conf,
								segment, segmentName);
					}
					
					if (count == 0) {
						Thread.sleep(10 * 60 * 1000);
						continue;
					}
					
					// add 2012-05-11
					if (isDistributionIndex) {
						File backPath = new File(conf.getPath(),
								backupSegmentName());
						MovePath.copyFile(segment, backPath);
						removeOldBackPath(conf.getPath());
						di.distribute();
						LOG.info("correct memory attribute end");
					}
				}
				Thread.sleep(10 * 60 * 1000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				// LOG.error(e.getMessage(),e);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private int initMinDocid(String tableName) throws FileNotFoundException,
			IOException, SQLException {
		Properties prop = new Properties();
		FileInputStream in = new FileInputStream(new File(
				conf.getMaxIdFilePath(), tableName));
		prop.load(in);
		in.close();
		int minID = Integer.parseInt(prop
				.getProperty(ProductCreateConstants.MAX_ID));
		return minID;
	}

	private boolean checkMaxForBuild(String url, String username,
			String password, String[] databases) throws FileNotFoundException,
			IOException, SQLException {
		String dbURL = url;
		boolean result = false;
		for (int i = 0; i < databases.length; i++) {
			int minID = initMinDocid(databases[i]);
			Connection conn = new MySQLConnectionImpl(dbURL + databases[i],
					username, password).openConnection();
			int maxID = productDataFromDB.getMaxProductID(conn);

			conn.close();

			String value = idsMap.get(databases[i]);
			if (value != null && value.equals(minID + "," + maxID)) {
				continue;
			} else {
				result = true;
				break;
			}
		}
		return result;
	}

	private void storeMaxID(String string, int maxID)
			throws FileNotFoundException, IOException {
		String path = this.conf.getMaxIdFilePath();
		Properties prop = new Properties();
		prop.put(ProductCreateConstants.MAX_ID, maxID);
		OutputStream os = new FileOutputStream(new File(path, string));
		prop.store(os, (new Date()).toString());
		os.close();
	}

	/**
	 * 部分索引名称
	 * 
	 * @return
	 */
	private String backupSegmentName() {
		SimpleDateFormat sd = new SimpleDateFormat("yyyyMMddHHmmss");
		return sd.format(new Date());
	}

	private void removeOldBackPath(String path) throws IOException,
			InterruptedException {
		File file = new File(path);
		File[] files = file.listFiles();
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -7);
		Date date = cal.getTime();
		SimpleDateFormat sd = new SimpleDateFormat("yyyyMMdd");
		String start = sd.format(date);
		for (int i = 0; i < files.length; i++) {
			if (files[i].getName().startsWith(start)) {
				MovePath.delete(files[i]);
			}
		}
	}

	public static void main(String[] args) {
		System.out.println(">>>>>>>>> begin created index server <<<<<<<< ");
		LOG.info(">>>>>>>>> begin created index server <<<<<<<< ");
		boolean distributionIndex = true;
		if (args.length > 1) {
			distributionIndex = Boolean.parseBoolean(args[1]);
		}
		IndexProductIntimeFromDB index = new IndexProductIntimeFromDB(args[0]);
		try {
		//	Stopwords.loadStopwords("stopwords.txt");
		//	Stopwords.loadSymbol("symbol.txt");
		//	Stopwords.loadHighFreq("highfreq.txt");

			index.build(distributionIndex);
		} catch (Exception e) {
			LOG.error("build exception", e);
			e.printStackTrace();
		}
	}

}

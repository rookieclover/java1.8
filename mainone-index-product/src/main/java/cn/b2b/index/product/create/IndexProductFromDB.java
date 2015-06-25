package cn.b2b.index.product.create;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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

public class IndexProductFromDB {
	private static final Logger LOG = Logger.getLogger("create");

	private static final String PRODUCT_INDEX_SEG_NAME = "product-index-db";

	private static boolean breakindex = false;

	private ProductCreateConfig conf;

	private static int BATCH_STEP = 10000;
	private ProductDataFromDB productDataFromDB = null;
	// 待审核、下架、已删除的不参与搜索
	private static final String PRODUCT_SQL = "select t.id as id, t.`userid` as userid, state&POWER(2,1)=POWER(2,1) as state, "
			+ // /*1 上架 0 下架*/
			" state&POWER(2,9)=POWER(2,9) auditstate, "
			+ /* 0 通过审核 1 未通过审核 */
			" d.title as title, d.`content` as content, t.`industryid` as industryid,"
			+ " d.`tradeid1` as tradeid, d.`keyword1` as keyword1, d.`keyword2` as keyword2, d.`keyword3` as keyword3,"
			+ " d.`province` as province, d.`city` as city, d.`picture1` as pic,"
			+ " d.`price` as price, d.`property` as property,d.`tradeproperty` as tradeproperty,"
			+ // 产品属性XML,/* P1 品牌*/
			" FROM_UNIXTIME(insertdate) as insertdate, FROM_UNIXTIME(updatedate) as updatedate, FROM_UNIXTIME(outtime) as outtime,"
			+ " concat('http://detail.b2b.cn/product/',t.id,'.html') as url "
			+ " from trade t,`tradedetail` d"
			+ " where t.id=d.id and t.id between ? and ? " + " order by t.id";

	/* and t.id in (724081227,723995325) */
	public IndexProductFromDB(String configpath) 
	{
		conf = new ProductCreateConfig();
		try {
			conf.loadConfig(configpath);

			Stopwords.loadStopwords("stopwords.txt");
			Stopwords.loadSymbol("symbol.txt");
			Stopwords.loadHighFreq("highfreq.txt");

			Stopwords.loadClassPath();
			productDataFromDB = new ProductDataFromDB();
			productDataFromDB.init(conf);
			QueryRuleManager.getRulePlugin(conf.getQueryRuleClass());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			LOG.error(e.getMessage(), e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
	}

	public void build(boolean isDistributionIndex) {
		try {
			String[] dbURL = conf.getMysqlUrls();
			String[] usernames = conf.getMysqlUsernames();
			String[] passwords = conf.getMysqlPasswords();
			String[] databasess = conf.getDatabases();
			// BufferedWriter writer1 = new BufferedWriter(new
			// FileWriter("/app/search/app/test/test1.txt"));
			// BufferedWriter writer2 = new BufferedWriter(new
			// FileWriter("/app/search/app/test/test2.txt"));
			String segmentName = conf.getSegmentName();
			if (segmentName == null) {
				segmentName = PRODUCT_INDEX_SEG_NAME;
			}
			File segment = FileTools.createFile(conf.getPath(), segmentName);
			IndexWriter iw = new IndexWriter(conf, segment, false);
			iw.setSimilarity(new MainOneSimilarity());
			long time1 = System.currentTimeMillis();
			int count = 0;
			// 开始建索引
			for (int k = 0; k < dbURL.length; k++) {
				String[] databases = databasess[k].split(",");
				for (int i = 0; i < databases.length; i++) {

					Connection conn = new MySQLConnectionImpl(dbURL[k]
							+ databases[i], usernames[k], passwords[k])
							.openConnection();
					int minID = productDataFromDB.getMinProductID(conn);
					int maxID = productDataFromDB.getMaxProductID(conn);
					PreparedStatement productps = conn
							.prepareStatement(PRODUCT_SQL);
					while (minID < maxID) {
						ResultSet productrs = null;
						try {
							LOG.info("DB NAME:" + databases[i]
									+ "\tcurrent minID:" + minID);
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
									LOG.info("DB NAME:" + databases[i]
											+ "\tIndex Product by " + count);
								}
								iw.addDocument(product.toDataBean());
								count = count + 1;
							}
						} catch (Exception e) {
							LOG.error("build:", e);
							e.printStackTrace();
						} finally {
							productrs.close();
						}
					}
					productps.close();
					storeMaxID(databases[i], maxID);
					conn.close();
				}
			}
			
			long totalEnd = System.currentTimeMillis();
			time1 = totalEnd;
			LOG.info("total time:" + (totalEnd - time1) + "\t count:"
					+ count);
			iw.close();
			// add 2012-05-11
			if (isDistributionIndex) {
				File backPath = new File(conf.getPath(), backupSegmentName());
				MovePath.copyFile(segment, backPath);
				removeOldBackPath(conf.getPath());

				DistributionIndex di = new DistributionIndex(conf, segment,
						segmentName);
				di.distribute();

				LOG.info("correct memory attribute end");
			} // add end
				// }
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
		}
	}

	private void storeMaxID(String string, int maxID) {
		try {
			String path = this.conf.getMaxIdFilePath();
			Properties prop = new Properties();
			prop.put(ProductCreateConstants.MAX_ID, maxID + "");
			OutputStream out = new FileOutputStream(new File(path, string));
			prop.store(out, new Date().toString());
			out.close();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
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

	private void removeOldBackPath(String path) {

		try {
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
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			LOG.error(e.getMessage(), e);
		}
	}

	public static void main(String[] args) {
		System.out.println(">>>>>>>>> begin created index server <<<<<<<< ");
		LOG.info(">>>>>>>>> begin created index server <<<<<<<< ");
		boolean distributionIndex = true;
		if (args.length > 1) {
			distributionIndex = Boolean.parseBoolean(args[1]);
		}
		IndexProductFromDB index = new IndexProductFromDB(args[0]);
		try {
			Stopwords.loadStopwords("stopwords.txt");
			Stopwords.loadSymbol("symbol.txt");
			Stopwords.loadHighFreq("highfreq.txt");

			index.build(distributionIndex);
		} catch (Exception e) {
			LOG.error("build exception", e);
			e.printStackTrace();
		}
	}

}

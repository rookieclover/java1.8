package cn.b2b.index.product.create;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.Logger;

import cn.b2b.common.search.query.rule.QueryRuleManager;
import cn.b2b.common.search.score.MainOneSimilarity;
import cn.b2b.common.search.transfer.MovePath;
import cn.b2b.common.search.util.DistributionIndex;
import cn.b2b.common.search.util.FileTools;
import cn.b2b.common.search.util.Stopwords;
import cn.b2b.create.index.create.IndexWriter;
import cn.b2b.index.product.create.bean.ProductBean;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class IndexProductIntimeFromMongoDB {

	private static final Logger LOG = Logger.getLogger("create");
	private static final String PRODUCT_INDEX_SEG_NAME = "product-index-intime-mongo";
	private static boolean breakindex = false;
	private ProductCreateConfig conf;
	private static int BATCH_STEP = 10000;
	private ProductDataFromDB productDataFromDB = null;
	private Map<String, String> idsMap = new HashMap<String, String>();
	private DistributionIndex di = null;
	private Calendar last_init_time = null;
	private int last_count = -1;
	private int WAIT_SECONDS = 1000 * 60 * 10;

	public IndexProductIntimeFromMongoDB(String configpath) {
		try {
			last_init_time = Calendar.getInstance();
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
	 * 
	 * @throws SQLException
	 */
	public void initFromDB() throws SQLException {
		Calendar cal = Calendar.getInstance();
		if (last_init_time.get(Calendar.DATE) != cal.get(Calendar.DATE)) {
			productDataFromDB.init(conf);
			last_init_time = Calendar.getInstance();
			LOG.info("Reload productDataFromDB init...");
		}
	}

	public void build(boolean isDistributionIndex) {
		while (true) {
			try {

				this.initFromDB();

				String segmentName = conf.getSegmentName();
				if (segmentName == null) {
					segmentName = PRODUCT_INDEX_SEG_NAME;
				}

				File segment = FileTools
						.createFile(conf.getPath(), segmentName);
				IndexWriter iw = new IndexWriter(conf, segment, false);
				iw.setSimilarity(new MainOneSimilarity());
				long begin = System.currentTimeMillis();
				int count = 0;

				Vector<ProductCreateConfig> configs = this.initMinDocid();

				for (ProductCreateConfig pcc : configs) {
					int userIdBegin = pcc.getUserIdBegin();
					int userIdEnd = pcc.getUserIdEnd();

					Mongo mongo = new Mongo(conf.getMongoDbHost(),
							conf.getMongoDbPort());
					DB mongoDB = mongo.getDB(conf.getMongoDbName());
					DBCollection collection = mongoDB.getCollection(conf
							.getMongoDbCollection());
					
					DBObject search = new BasicDBObject();
					DBObject userIdbetween = new BasicDBObject();
					userIdbetween.put("$gte", userIdBegin);
					userIdbetween.put("$lte", userIdEnd);
					
					DBObject idGreat = new BasicDBObject();
					idGreat.put("$gt", pcc.getMaxId());

					search.put("uid", userIdbetween );
					search.put("_id", idGreat);
				//	search.put("_id", 714576754);
					
					LOG.info(search.toString());
					DBCursor cursor = collection.find(search);
					while (cursor.hasNext()) {
						count++;
						DBObject obj = cursor.next();
						ProductBean bean = productDataFromDB
								.getProductData(obj);
						if (count % 1000 == 0) {
							LOG.info("Create product index record count "
									+ count);
						}
						//System.out.println(bean.toDataBean());
						iw.addDocument(bean.toDataBean());
					}
				}

				long totalEnd = System.currentTimeMillis();
				LOG.info("total time:" + (totalEnd - begin) + "ms\t count:"
						+ count);
				iw.close();
				if (di == null) {
					di = new DistributionIndex(conf, segment, segmentName);
				}
				if (count == 0) {
					LOG.info("product index not is updated");
					Thread.sleep(WAIT_SECONDS);
					continue;
				}
				try {
					if (isDistributionIndex) {
						File backPath = new File(conf.getPath(),
								backupSegmentName());
						MovePath.copyFile(segment, backPath);
						removeOldBackPath(conf.getPath());
						di.distribute();
						LOG.info("correct memory attribute end");
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				} finally {
					Thread.sleep(WAIT_SECONDS);
				}
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
			} catch (java.lang.RuntimeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {

			}
		}

	}

	private Vector<ProductCreateConfig> initMinDocid()
			throws FileNotFoundException, IOException {
		String path = this.conf.getMaxIdFilePath();
		Vector<ProductCreateConfig> list = new Vector<ProductCreateConfig>();
		File dir = new File(path);

		if (dir.isDirectory()) {
			String[] files = dir.list();
			for (String f : files) {
				if(f.indexOf("MONGO_MAXID_")!=-1)
				{
					Properties prop = new Properties();
					FileInputStream in = new FileInputStream(new File(path + File.separatorChar+ f));
					prop.load(in); 
					in.close();
					int minId = Integer.parseInt(prop.getProperty(ProductCreateConstants.MAX_ID));
					int userIdBegin = Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_USERID_BEGIN));
					int userIdEnd = Integer.parseInt(prop.getProperty(ProductCreateConstants.MONGO_DB_USERID_END));
					
					
					ProductCreateConfig c = new ProductCreateConfig();
					c.setMaxId(minId);
					c.setUserIdBegin(userIdBegin);
					c.setUserIdEnd(userIdEnd);
					list.add(c);
				}
			}
			/*
			 * Properties prop = new Properties(); FileInputStream in = new
			 * FileInputStream(new File( conf.getMaxIdFilePath()));
			 * prop.load(in); in.close();
			 * 
			 * int minID = Integer.parseInt(prop
			 * .getProperty(ProductCreateConstants.MAX_ID));
			 */
		}
		return list;
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
		IndexProductIntimeFromMongoDB index = new IndexProductIntimeFromMongoDB(
				args[0]);
		try {
			// Stopwords.loadStopwords("stopwords.txt");
			// Stopwords.loadSymbol("symbol.txt");
			// Stopwords.loadHighFreq("highfreq.txt");

			index.build(distributionIndex);
		} catch (Exception e) {
			LOG.error("build exception", e);
			e.printStackTrace();
		}
	}

}

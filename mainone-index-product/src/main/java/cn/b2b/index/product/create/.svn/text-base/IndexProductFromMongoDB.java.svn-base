package cn.b2b.index.product.create;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class IndexProductFromMongoDB {
	private static final Logger LOG = Logger.getLogger("create");

	private static final String PRODUCT_INDEX_SEG_NAME = "product-index-mongo";
	private ProductCreateConfig conf;
	private ProductDataFromDB productDataFromDB = null;

	public IndexProductFromMongoDB(String configpath) {
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

			String segmentName = conf.getSegmentName();
			if (segmentName == null) {
				segmentName = PRODUCT_INDEX_SEG_NAME;
			}
			File segment = FileTools.createFile(conf.getPath(), segmentName);
			IndexWriter iw = new IndexWriter(conf, segment, false);
			iw.setSimilarity(new MainOneSimilarity());
			long begin = System.currentTimeMillis();
			int count = 0;
			Mongo mongo = new Mongo(conf.getMongoDbHost(),
					conf.getMongoDbPort());
			DB mongoDB = mongo.getDB(conf.getMongoDbName());
			DBCollection collection = mongoDB.getCollection(conf
					.getMongoDbCollection());

			int userIdBegin = conf.getUserIdBegin();
			int userIdEnd = conf.getUserIdEnd();

			DBObject search = new BasicDBObject();
			DBObject idbetween = new BasicDBObject();
			idbetween.put("$gte", userIdBegin);
			idbetween.put("$lte", userIdEnd);
			search.put("uid", idbetween);
			LOG.info(search.toString());
			DBCursor cursor = collection.find(search);
			int maxId = 0;
			while (cursor.hasNext()) {
				count++;
				DBObject obj = cursor.next();
				ProductBean bean =  productDataFromDB.getProductData(obj);
				if(bean.getNotfindproperties()==1)
				{
					LOG.info("Not find properties from product id:\t"+ bean.getId());
				}
				maxId = maxId < bean.getId() ? bean.getId() : maxId;
				if (count % 1000 == 0) {
					LOG.info("Create product index recoed " + count);
				}
				iw.addDocument(bean.toDataBean());
			}
			storeMaxID(maxId, userIdBegin, userIdEnd);
			long totalEnd = System.currentTimeMillis();
			LOG.info("total time:" + (totalEnd - begin) + "ms\t count:" + count);
			iw.close();
			if (isDistributionIndex) {
				File backPath = new File(conf.getPath(), backupSegmentName());
				MovePath.copyFile(segment, backPath);
				removeOldBackPath(conf.getPath());
				DistributionIndex di = new DistributionIndex(conf, segment,
						segmentName);
				di.distribute();
				LOG.info("correct memory attribute end");
			}
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
		}
	}

	private void storeMaxID(int maxID, int userIdBegin, int userIdEnd) {
		try {
			String path = this.conf.getMaxIdFilePath(userIdBegin, userIdEnd);
			Properties prop = new Properties();
			prop.put(ProductCreateConstants.MONGO_DB_USERID_BEGIN, userIdBegin
					+ "");
			prop.put(ProductCreateConstants.MONGO_DB_USERID_END, userIdEnd + "");
			prop.put(ProductCreateConstants.MAX_ID, maxID + "");
			OutputStream out = new FileOutputStream(new File(path));
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
		IndexProductFromMongoDB index = new IndexProductFromMongoDB(args[0]);
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

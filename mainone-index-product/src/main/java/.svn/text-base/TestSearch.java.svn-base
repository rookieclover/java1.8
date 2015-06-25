import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.score.ScorePluginLoader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BitVector;

import cn.b2b.common.search.IndexConfig;
import cn.b2b.common.search.bean.Hit;
import cn.b2b.common.search.bean.ProductHits;
import cn.b2b.common.search.bean.company.ClusterBean;
import cn.b2b.common.search.bean.product.ProductQueryParam;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.filter.QueryFilters;
import cn.b2b.common.search.util.Constants;
import cn.b2b.index.product.client.IProductUpdateBean;
import cn.b2b.index.product.client.ProductUpdateBeanImpl;
import cn.b2b.index.product.common.ProductConstants;
import cn.b2b.index.product.index.ProductQueryOptimizer;
import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.ScoreParam;

public class TestSearch {
	public static void main(String[] args) throws Exception {

		String path = "E:\\jar\\product-index-mongo\\product-index-mongo\\index";
		List<IndexReader> readers = new ArrayList<IndexReader>();
		IndexReader reader1 = IndexReader.open(path);

		readers.add(reader1);

		System.out.println("DOC2 TOTAL:\t" + reader1.maxDoc());

		MultiReader reader = new MultiReader(
				readers.toArray(new IndexReader[0]));
		IndexConfig config = new IndexConfig();
		config.initConfig("D:\\mainonecode\\mainonesearch\\mainone-index-product\\config\\index\\index.conf");
		QueryFilters
				.loadFilters("D:\\mainonecode\\mainonesearch\\mainone-index-product\\config\\index\\filter");
		IndexSearcher searcher = new IndexSearcher(reader, false, config, null,
				null);
		// searcher.delDocByDocnum(new int[]{714575965});
		// searcher.unDeleteDocByDocNum(714575965);
		ScorePluginLoader.load(reader);
		// ScoreParam scoreParam = new ScoreParam();
		// scoreParam.setSearchType((byte)1);
		// // scoreParam.setSearchType((byte)5);
		// scoreParam.setSortType((byte)-1);
		// scoreParam.setLongitude(104.08022f);
		// scoreParam.setLatitude(30.635338f);
		// scoreParam.setLbs(3);
		ProductQueryOptimizer optimizer = new ProductQueryOptimizer(16, 0.05f);
		BufferedReader strin = new BufferedReader(new InputStreamReader(
				System.in));
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				"e:/result.txt"));

		ProductQueryParam queryParam = makeParam();

		long begin = System.currentTimeMillis();
		System.out.println("搜索开始\t");
		ProductHits hits = (ProductHits) optimizer.optimize(searcher,
				queryParam);
		long end = System.currentTimeMillis();
		System.out.println("搜索用时:\t" + (end - begin));

		System.out.println("搜索总计:\t" + hits.getTotal());
		System.out.println("搜索总计:\t" + hits.getGrpHits());

		BooleanQuery original = QueryFilters.filter(queryParam.getQuery(),
				Constants.SLOP_SCORE_CVM_CLASS);
		ClusterBean[] trades = hits.getIndustryids();
		for (int i = 0; i < trades.length; i++) {
			System.out.println(trades[i].getId() + "\t" + trades[i].getNum());
		}
		Hit[] docs = hits.getHits();
		for (int i = 0; i < docs.length; i++) {
			writer.write(docs[i].getIndexDocNo() + "\n");
			Properties prop = searcher.getDetailSummary(
					docs[i].getIndexDocNo(), "", 1, 0);
			BitVector haspic = (BitVector) MemoryFieldCache.get("haspic");
			// System.out.println(prop);

			System.out.println(docs[i].getIndexDocNo() + "\t"
					+ docs[i].getScore() + "\t" + prop.getProperty("title")
					+ "\t" + prop.getProperty("keyword") + "haspic:\t"
					+ haspic.get(docs[i].getIndexDocNo()) + "\tindustryid:\t"
					+ prop.getProperty("industryid") + "\t["
					+ prop.getProperty("spec") + "\t"
					+ prop.getProperty("unit") + "\t"
					+ prop.getProperty("price") + "\t"
					+ prop.getProperty("mincount") + "]"

			);
			// Explanation explain = searcher.explain(original,
			// docs[i].getIndexDocNo());
			// System.out.println(explain.toString());
		}
		writer.flush();
		// Thread.currentThread().join();
	}

	private static ProductQueryParam makeParam() {
		Query cnquery = new Query();
		// cnquery.addRequiredTerm("秋|1.0/梨膏|3.0/", "DEFAULT", true);

		// cnquery.addRequiredTerm("电源|1.0/手机|1.0/", "DEFAULT", true);
		// cnquery.addRequiredTerm("塑料|8.526528/", "DEFAULT", true);
		// cnquery.addRequiredTerm("玛瑙|1.0/樱桃|1.0/一二三四五六|1.0/玛瑙|1.0/樱桃|1.0/一二三四五六|1.0/",
		// "DEFAULT", true);
		// cnquery.addRequiredTerm("板栗|1.0/免费|1.0/", "DEFAULT", true);
		// cnquery.addRequiredTerm("vishayn|1.0/", "DEFAULT", true);

		/*
		 * QUERY:+(+(((+anchor:价格 +anchor:厂家 +anchor:器 +anchor:眼 +anchor:工厂
		 * +anchor:不锈钢 +anchor:双头 +anchor:生产 +anchor:洗 +anchor:紧急 +anchor:立式
		 * +anchor:登 +anchor:固 +anchor:，)^200.0) ((+content:价格 +content:厂家
		 * +content:器 +content:眼 +content:工厂 +content:不锈钢 +content:双头
		 * +content:生产 +content:洗 +content:紧急 +content:立式 +content:登 +content:固
		 * +content:，)^5.0))) 08 五月 2014 16:53:14 | QUERY_AND:-(+(+(((+anchor:价格
		 * +anchor:厂家 +anchor:器 +anchor:眼 +anchor:工厂 +anchor:不锈钢 +anchor:双头
		 * +anchor:生产 +anchor:洗 +anchor:紧急 +anchor:立式 +anchor:登 +anchor:固
		 * +anchor:，)^200.0) ((+content:价格 +content:厂家 +content:器 +content:眼
		 * +content:工厂 +content:不锈钢 +content:双头 +content:生产 +content:洗
		 * +content:紧急 +content:立式 +content:登 +content:固 +content:，)^5.0))))
		 * +(+(((+anchor:价格 +anchor:厂家)^200.0) ((+content:价格 +content:厂家)^5.0)))
		 */

		cnquery.addRequiredTerm("猴|1.0/", "DEFAULT", true);

		// cnquery.addRequiredTerm("价格|1.0/厂家|1.0/器|1.0/眼|1.0/工厂|1.0/不锈钢|1.0/双头|1.0/生产|1.0/洗|1.0/紧急|1.0/立式|1.0/登|1.0/固|1.0/",
		// "DEFAULT", true);
		// cnquery.addRequiredTerm("二手|1.0/电脑|1.0/", "DEFAULT", true);
		// cnquery.addRequiredTerm("价格|1.0/厂家|1.0/器|1.0/眼|1.0/工厂|1.0/不锈钢|1.0/双头|1.0/生产|1.0/洗|1.0/紧急|1.0/立式|1.0/登|1.0/固|1.0/",
		// "DEFAULT", true);

		// cnquery.addRequiredTerm("柜式|1.0/空调|1.0/立式|1.0/", "DEFAULT", true);
		// cnquery.addRequiredTerm("板栗|1.0/八宝|1.0/燕山|1.0/", "DEFAULT", true);

		// cnquery.addRequiredTerm("广告|1.0/火机|1.0/打|1.0/一次性|1.0/2|1.0/",
		// "DEFAULT", true);
		// cnquery.addRequiredTerm("智能|1.0/移动|1.0/平板|1.0/电脑|1.0/手机|1.0/电源|1.0/",
		// "DEFAULT", true);
		// cnquery.addRequiredTerm("电脑|1.0/手机|1.0/电源|1.0/济南|0.2/金|1.0/得利|1.0/快餐|1.0/加盟|1.0/",
		// "DEFAULT", true);
		// cnquery.addRequiredTerm("18863", "industryid", true);
		// cnquery.addRequiredTerm("14501", "industryid", true);
		ProductQueryParam param = new ProductQueryParam(cnquery);
		Query notQuery = new Query();
		// if (searchItem.getAttrKeyWord() != null &&
		// searchItem.getAttrKeyWord().trim().length() > 0) {
		// attrQuery = SearchQuery.parse(searchItem.getAttrKeyWord());
		// }
		param.setNotQuery(notQuery);
		param.setHitsNum(15);
		param.setSortType((byte) -1);
		param.setSearchType((byte) 1);
		param.setArea(-1);
		param.setProvince(-1);
		param.setCity(-1);
		param.setMemberLevel(-1);
		param.setCompanyLicense((byte) -1);
		param.setDatatype((byte) -1);
		param.setTradeLevel(1);
		param.setHaspic((byte) 0);

		return param;
	}

	public static String getTradeid(int[] ids) {
		String result = "";
		for (int i = 0; i < ids.length; i++) {
			result += " " + ids[i];
		}
		return result;
	}

	public static Map<String, String> getUpdateMap() {
		IProductUpdateBean updateBean = new ProductUpdateBeanImpl();
		updateBean.setProductid(217073);

		updateBean.setTradeid("1009792 1005218 1009000 1005200");
		updateBean.setMemlevel(3);
		updateBean.setState(1);
		updateBean.setIndustryid(10000);
		return getUpdateMap(updateBean);

	}

	private static Map<String, String> getUpdateMap(
			IProductUpdateBean updateBean) {

		Map<String, String> updateMemoryData = new HashMap<String, String>();

		if (updateBean.getTradeid() != null
				&& updateBean.getTradeid().length() > 0) {
			updateMemoryData.put("tradeid", updateBean.getTradeid());
		}
		if (updateBean.getArea() != -1) {
			updateMemoryData.put("area", updateBean.getArea() + "");
		}
		if (updateBean.getCity() != -1) {
			updateMemoryData.put("city", updateBean.getCity() + "");
		}
		if (updateBean.getIndustryid() != -1) {
			updateMemoryData.put("industryid", updateBean.getIndustryid() + "");
		}
		if (updateBean.getLicense() != -1) {
			updateMemoryData.put("license", updateBean.getLicense() + "");
		}
		if (updateBean.getMemlevel() != -1) {
			updateMemoryData.put("memlevel", updateBean.getMemlevel() + "");
		}

		if (updateBean.getProvince() != -1) {
			updateMemoryData.put("province", updateBean.getProvince() + "");
		}
		if (updateBean.getUpdatedate() != null) {
			updateMemoryData.put("updatedate", updateBean.getUpdatedate()
					.getTime() + "");
		}
		if (updateBean.getState() != -1) {
			updateMemoryData.put("state", updateBean.getState() + "");
		}
		return updateMemoryData;

	}
}

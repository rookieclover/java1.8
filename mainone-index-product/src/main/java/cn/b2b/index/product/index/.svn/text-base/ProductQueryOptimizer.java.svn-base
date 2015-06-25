package cn.b2b.index.product.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BitVector;

import cn.b2b.common.rpc.io.Writable;
import cn.b2b.common.search.bean.ExplainParam;
import cn.b2b.common.search.bean.Hit;
import cn.b2b.common.search.bean.HitDetails;
import cn.b2b.common.search.bean.ProductHits;
import cn.b2b.common.search.bean.company.ClusterBean;
import cn.b2b.common.search.bean.product.ProductQueryParam;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.filter.QueryFilters;
import cn.b2b.common.search.util.Constants;
import cn.b2b.common.search.util.UTF8;
import cn.b2b.index.product.common.ProductConstants;
import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.QueryOptimizer;
import cn.b2b.lucene.search.plugin.ScoreParam;

public class ProductQueryOptimizer extends QueryOptimizer {

	public ProductQueryOptimizer(int cacheSize, float threshold) {
		super(cacheSize, threshold, new ProductIndexScorer());
		this.setSimilarity(new DefaultSimilarity());
	}

	@Override
	public Writable optimize(Searcher searcher, Writable param)
			throws Exception {

		byte queryType = ProductConstants.QUERY_TYPE_MUST_AND;
		//queryType = ProductConstants.QUERY_TYPE_MUST_OR;
        long begin = System.currentTimeMillis();

		ProductQueryParam queryParam = (ProductQueryParam) param;
		Query oriquery = queryParam.getQuery();
		
		BooleanQuery original = QueryFilters.filter(oriquery,
				queryType);
		
		BooleanQuery oriQuery = new BooleanQuery();
		BooleanQuery query = new BooleanQuery();
		BooleanQuery filterQuery = filterQuery(searcher, original, oriQuery);
		
		query.add(oriQuery, BooleanClause.Occur.MUST);

		Query attrQuery = queryParam.getAttrQuery();
		BooleanQuery attribute = QueryFilters.filter(attrQuery,
				Constants.SLOP_SCORE_MATCH_CLASS);
		if (attribute.getClauses() != null && attribute.getClauses().length > 0) {
			query.add(attribute, BooleanClause.Occur.MUST);
		}

		Query notquery = queryParam.getNotQuery();
		BooleanQuery notQuery = QueryFilters.filter(notquery,
				Constants.SLOP_SCORE_MATCH_CLASS);
		if (notQuery.getClauses() != null && notQuery.getClauses().length > 0) {
			query.add(notQuery, Occur.MUST_NOT);
		}
	
		Filter filter = null;
		if (filterQuery != null) {
			synchronized (cache) { // check cache
				filter = (Filter) cache.get(filterQuery);
			}
			if (filter == null) { // miss
				filter = new QueryFilter(filterQuery); // construct new entry
				synchronized (cache) {
					cache.put(filterQuery, filter); // cache it
				}
			}
		}
		
		IndexSearcher.LOG.info("QUERY:" + query);
		
		ScoreParam scoreParam = new ScoreParam();
		scoreParam.setSortType(queryParam.getSortType());
		scoreParam.setSearchType(queryParam.getSearchType());
		scoreParam.setTradeLevel(queryParam.getTradeLevel());
		scoreParam.setScorePercent(2.0f);
		// store segment query word
		query.setQueryStr(original.getQueryStr());
		TopDocs[] topDocss = new TopDocs[2];
		TopDocs topDocs = null;
		TopDocs or = null;
		BitVector bites = makeBitQuery(searcher.maxDoc(), queryParam);

        long end = System.currentTimeMillis();

		topDocs = searcher.searchProduct(query, filter, indexScorer,
				queryParam.getHitsNum(), scoreParam, bites);
		
		
		IndexSearcher.LOG.info("totalhits:\t"+ topDocs.totalHits);
		

		if(queryType == ProductConstants.QUERY_TYPE_MUST_AND)
		{
			scoreParam.setScorePercent(0.5f);
			BooleanQuery bqs = new BooleanQuery();
			BooleanQuery query_or =  QueryFilters.filter(oriquery,ProductConstants.QUERY_TYPE_MUST_AND_MUST_HAS_NEED_WORD);
			bqs.add(query,Occur.MUST_NOT);
			bqs.add(query_or,Occur.MUST);
			IndexSearcher.LOG.info("QUERY_AND:" + bqs);
			or = searcher.searchProduct(bqs, filter, indexScorer,
					queryParam.getHitsNum(), scoreParam, bites);
			topDocss[0] = topDocs;
			topDocss[1] = or;
			//topDocss[1] = null;
			return this.serializeHits(topDocss);
		}
		return this.serializeHits(topDocs);
	}

	private BooleanQuery filterQuery(Searcher searcher, BooleanQuery original,
			BooleanQuery query) throws IOException {
		BooleanQuery filterQuery = null;
		BooleanClause[] clauses = original.getClauses();
		for (int i = 0; i < clauses.length; i++) {
			BooleanClause c = clauses[i];
			if (c.isRequired() // required
					&& c.getQuery().getBoost() == 0.0f // boost is zero
					&& c.getQuery() instanceof TermQuery // TermQuery
					&& (searcher.docFreq(((TermQuery) c.getQuery()).getTerm()) / (float) searcher
							.maxDoc()) >= threshold) { // check threshold
				if (filterQuery == null)
					filterQuery = new BooleanQuery();
				// lucene1.4.3 -> lucene2.0.0
				// filterQuery.add(c.getQuery(), true, false); // filter it
				filterQuery.add(c.getQuery(), BooleanClause.Occur.MUST); // filter
																			// it
			} else {
				query.add(c); // query it
			}
		}
		return filterQuery;
	}

	protected Writable serializeHits(TopDocs[] topDocss) {

		ProductHits hs = null;
		List<Hit>  hits = new ArrayList<Hit>();
		
		ArrayList<ClusterBean> clusterTradeids = new ArrayList<ClusterBean>();
		ArrayList<ClusterBean> clusterIndustryids = new ArrayList<ClusterBean>();
		
		int totalHits = 0 ; 
		int totalClustHits = 0 ;

	     Map<Integer, Integer> tradeMap = new HashMap<Integer, Integer>();
	     Map<Integer, Integer> industryMap = new HashMap<Integer, Integer>();

		for(TopDocs topDocs : topDocss)
		{
			if(topDocs==null)break;
			ScoreDoc[] scoreDocs = topDocs.scoreDocs;
			int length = scoreDocs.length;
			
			for (int i = 0; i < length; i++) {
				int doc = scoreDocs[i].doc;
				hits.add(new Hit(doc, scoreDocs[i].pscore, scoreDocs[i].intime));
			}
			
			totalHits +=topDocs.totalHits;
			totalClustHits+= topDocs.totalClustHits;
			
            ClusterBean[] tradeids = topDocs.getTradeids();
            for (int k = 0; k < tradeids.length; k++) {
                if (tradeMap.get(tradeids[k].getId()) != null) {
                    int num = tradeMap.get(tradeids[k].getId()) + tradeids[k].getNum();
                    tradeMap.put(tradeids[k].getId(), num);
                } else {
                    tradeMap.put(tradeids[k].getId(), tradeids[k].getNum());
                }
            }
            
            ClusterBean[] industryids = topDocs.getIndustryids();
            for (int k = 0; k < industryids.length; k++) {
                if (industryMap.get(industryids[k].getId()) != null) {
                    int num = industryMap.get(industryids[k].getId()) + industryids[k].getNum();
                    industryMap.put(industryids[k].getId(), num);
                } else {
                    industryMap.put(industryids[k].getId(), industryids[k].getNum());
                }
            }
		}      
        Set<Integer> ikeySet = industryMap.keySet();
        Iterator<Integer> iiter = ikeySet.iterator();
        while (iiter.hasNext()) {
            int key = iiter.next();
            int value = industryMap.get(key);
            clusterIndustryids.add(new ClusterBean(key, value));
        }
        
        ikeySet = tradeMap.keySet();
        iiter = ikeySet.iterator();
        while (iiter.hasNext()) {
            int key = iiter.next();
            int value = tradeMap.get(key);
            clusterTradeids.add(new ClusterBean(key, value));
        }
        
        iiter = null;ikeySet= null;
        industryMap.clear();
        tradeMap.clear();
        industryMap = null;tradeMap=null;

		hs=  new ProductHits(totalHits, totalClustHits, hits.toArray(new Hit[0]),
				clusterTradeids.toArray(new ClusterBean[0]), clusterIndustryids.toArray(new ClusterBean[0]));
		return hs;
	}
	
	protected Writable serializeHits(TopDocs topDocs) {

        long begin = System.currentTimeMillis();

		ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		int length = scoreDocs.length;
		Hit[] hits = new Hit[length];
		for (int i = 0; i < length; i++) {
			int doc = scoreDocs[i].doc;
			hits[i] = new Hit(doc, scoreDocs[i].pscore, scoreDocs[i].intime);
		}
		ProductHits hs=  new ProductHits(topDocs.totalHits, topDocs.totalClustHits, hits,
				topDocs.getTradeids(), topDocs.getIndustryids());
		
        long end = System.currentTimeMillis();
		
		return hs;
	}

	final static int CITY_BIT = 1;
	final static int MEMLEVEL_BIT = 2;
	final static int DATATYPE_BIT = 4;
	final static int SIZE_BIT = 8;
	final static int OPERATION_BIT = 16;

	private BitVector makeBitQuery(int maxdoc, ProductQueryParam queryParam) {
		BitVector states = (BitVector) MemoryFieldCache.get("state");

		BitVector bites = new BitVector(maxdoc);
		bites.or(states);

		if (queryParam.getCompanyLicense() != -1) {
			BitVector licenses = (BitVector) MemoryFieldCache.get("license");
			bites.and(licenses);
		}

		if (queryParam.getHaspic() != -1 && queryParam.getHaspic() == 1) {
			BitVector artificials = (BitVector) MemoryFieldCache.get("haspic");
			bites.and(artificials);
		}

		int querybit = 0;
		int[] citys = new int[0];
		int city = 0;
		if (queryParam.getArea() != -1) {
			querybit += CITY_BIT;
			city = queryParam.getArea();
			citys = (int[]) MemoryFieldCache.get("area");
		}
		if (queryParam.getProvince() != -1) {
			querybit += CITY_BIT;
			city = queryParam.getProvince();
			citys = (int[]) MemoryFieldCache.get("province");
		}
		if (queryParam.getCity() != -1) {

			querybit += CITY_BIT;
			city = queryParam.getCity();
			citys = (int[]) MemoryFieldCache.get("city");
		}

		if (queryParam.getMemberLevel() != -1) {
			querybit += MEMLEVEL_BIT;
		}

		if (queryParam.getDatatype() > 0) {
			querybit += DATATYPE_BIT;
		}

		byte[] memlevels = (byte[]) MemoryFieldCache.get("memlevel");
		byte[] datatypes = (byte[]) MemoryFieldCache.get("datatype");
		if (querybit == CITY_BIT) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (citys[i] != city) {
						bites.clear(i);
					}
				}
			}
		} else if (querybit == (CITY_BIT + MEMLEVEL_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {

					if (citys[i] != city
							|| memlevels[i] != queryParam.getMemberLevel()) {
						bites.clear(i);
					}
				}
			}
		} else if (querybit == (CITY_BIT + MEMLEVEL_BIT + DATATYPE_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (citys[i] != city
							|| memlevels[i] != queryParam.getMemberLevel()
							|| datatypes[i] != queryParam.getDatatype()) {
						bites.clear(i);
					}
				}
			}
		} else if (querybit == (CITY_BIT + DATATYPE_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (citys[i] != city
							|| datatypes[i] != queryParam.getDatatype()) {
						bites.clear(i);
					}
				}
			}

		} else if (querybit == (MEMLEVEL_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (memlevels[i] != queryParam.getMemberLevel()) {
						bites.clear(i);
					}
				}
			}

		} else if (querybit == (MEMLEVEL_BIT + DATATYPE_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (memlevels[i] != queryParam.getMemberLevel()
							|| datatypes[i] != queryParam.getDatatype()) {
						bites.clear(i);
					}
				}
			}

		} else if (querybit == (DATATYPE_BIT)) {
			for (int i = 0; i < maxdoc; i++) {
				if (bites.get(i)) {
					if (datatypes[i] != queryParam.getDatatype()) {
						bites.clear(i);
					}
				}
			}
		}
		//
		// if (queryParam.getArea() > -1) {
		// bites = new BitVector(maxdoc);
		// int[] areas = (int[])MemoryFieldCache.get("area");
		// for (int i = 0; i < areas.length && i < maxdoc; i++) {
		// if (areas[i] == queryParam.getArea() && bites.get(i)) {
		// bites.set(i);
		// } else {
		// bites.clear(i);
		// }
		// }
		// }
		// if (queryParam.getProvince() != -1) {
		// int[] provinces = (int[])MemoryFieldCache.get("province");
		// for (int i = 0; i < provinces.length && i < maxdoc; i++) {
		// if (provinces[i] == queryParam.getProvince() && bites.get(i)) {
		// bites.set(i);
		// } else {
		// bites.clear(i);
		// }
		// }
		//
		// }
		// if (queryParam.getCity() != -1) {
		// int[] citys = (int[])MemoryFieldCache.get("city");
		// for (int i = 0; i < citys.length && i < maxdoc; i++) {
		// if (citys[i] == queryParam.getCity() && bites.get(i)) {
		// bites.set(i);
		// } else {
		// bites.clear(i);
		// }
		// }
		//
		// }
		//
		// if (queryParam.getMemberLevel() != -1) {
		// byte[] memlevels = (byte[])MemoryFieldCache.get("memlevel");
		// for (int i = 0; i < memlevels.length && i < maxdoc; i++) {
		// if (memlevels[i] == queryParam.getMemberLevel() && bites.get(i)) {
		// bites.set(i);
		// } else {
		// bites.clear(i);
		// }
		// }
		// }
		//
		// if (queryParam.getDatatype() > 0) {
		// byte[] provinces = (byte[])MemoryFieldCache.get("datatype");
		// for (int i = 0; i < provinces.length && i < maxdoc; i++) {
		// if (provinces[i] == queryParam.getDatatype() && bites.get(i)) {
		// bites.set(i);
		// } else {
		// bites.clear(i);
		// }
		// }
		// }

		return bites;
	}

	@Override
	public Writable serializeDetails(Properties p) {
		HitDetails result = new HitDetails();
		result.parse(p);
		return result;
	}

	@Override
	public int optimizeDeleteByWord(IndexSearcher searcher, Query query,
			byte datatype, byte memlevel) throws Exception {
		int maxdoc = searcher.maxDoc();
		BitVector bites = makeDelBits(datatype, memlevel, maxdoc);
		BooleanQuery original = QueryFilters.filter(query,
				Constants.SLOP_SCORE_MATCH_CLASS);
		int result = searcher.deleteByWord(original, bites);
		return result;
	}

	private BitVector makeDelBits(byte datatype, byte memlevel, int maxdoc)
			throws IOException {
		BitVector bites = null;

		if (datatype != -1) {
			bites = new BitVector(maxdoc);
			byte[] datatypes = (byte[]) MemoryFieldCache.get("datatype");
			for (int i = 0; i < datatypes.length && i < maxdoc; i++) {
				if (datatypes[i] == datatype) {
					bites.set(i);
				} else {
					bites.clear(i);
				}
			}
		}
		if (memlevel != -1) {
			if (bites != null) {
				byte[] memlevels = (byte[]) MemoryFieldCache.get("memlevel");
				for (int i = 0; i < memlevels.length && i < maxdoc; i++) {
					if (memlevels[i] == memlevel && bites.get(i)) {
						bites.set(i);
					} else {
						bites.clear(i);
					}
				}
			} else {
				bites = new BitVector(maxdoc);
				byte[] memlevels = (byte[]) MemoryFieldCache.get("memlevel");
				for (int i = 0; i < memlevels.length && i < maxdoc; i++) {
					if (memlevels[i] == memlevel) {
						bites.set(i);
					} else {
						bites.clear(i);
					}
				}

			}
		}
		return bites;
	}

	@Override
	public Writable getIdByDelWord(IndexSearcher searcher, Query query,
			int num, byte datatype, byte memlevel) throws Exception {
		int maxdoc = searcher.maxDoc();
		BitVector bites = makeDelBits(datatype, memlevel, maxdoc);
		BooleanQuery original = QueryFilters.filter(query,
				Constants.SLOP_SCORE_MATCH_CLASS);
		TopDocs topDocs = searcher.getIdByDelWord(original, num, bites);
		return serializeHits(topDocs);
	}

	@Override
	public Writable explain(IndexSearcher searcher, Writable param)
			throws Exception {

		ExplainParam queryParam = (ExplainParam) param;
		Query oriquery = queryParam.getQuery();
		BooleanQuery original = QueryFilters.filter(oriquery,
				Constants.SLOP_SCORE_CVM_CLASS);
		BooleanQuery query = new BooleanQuery();
		BooleanQuery filterQuery = null;
		BooleanClause[] clauses = original.getClauses();
		for (int i = 0; i < clauses.length; i++) {
			BooleanClause c = clauses[i];
			if (c.isRequired() // required
					&& c.getQuery().getBoost() == 0.0f // boost is zero
					&& c.getQuery() instanceof TermQuery // TermQuery
					&& (searcher.docFreq(((TermQuery) c.getQuery()).getTerm()) / (float) searcher
							.maxDoc()) >= threshold) { // check threshold
				if (filterQuery == null)
					filterQuery = new BooleanQuery();
				// lucene1.4.3 -> lucene2.0.0
				// filterQuery.add(c.getQuery(), true, false); // filter it
				filterQuery.add(c.getQuery(), BooleanClause.Occur.MUST); // filter
																			// it
			} else {
				query.add(c); // query it
			}
		}

		Filter filter = null;
		if (filterQuery != null) {
			synchronized (cache) { // check cache
				filter = (Filter) cache.get(filterQuery);
			}
			if (filter == null) { // miss
				filter = new QueryFilter(filterQuery); // construct new entry
				synchronized (cache) {
					cache.put(filterQuery, filter); // cache it
				}
			}
		}
		ScoreParam scoreParam = new ScoreParam();
		// store segment query word
		query.setQueryStr(original.getQueryStr());

		float[] ranks = (float[]) MemoryFieldCache.get("rank");
		Explanation explain = searcher.explain(query, queryParam.getDoc());
		return new UTF8("RANK:" + ranks[queryParam.getDoc()] + "\t"
				+ explain.toString());
	}
}

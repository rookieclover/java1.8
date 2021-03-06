package cn.b2b.index.product.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import cn.b2b.client.search.client.Searcher;
import cn.b2b.common.rpc.io.BooleanWritable;
import cn.b2b.common.rpc.io.IntWritable;
import cn.b2b.common.rpc.io.Writable;
import cn.b2b.common.search.bean.DataUpdateBean;
import cn.b2b.common.search.bean.DelDocsByWordBean;
import cn.b2b.common.search.bean.DocumentDelParam;
import cn.b2b.common.search.bean.Hit;
import cn.b2b.common.search.bean.HitDetails;
import cn.b2b.common.search.bean.Hits;
import cn.b2b.common.search.bean.IndexServers.IndexInetSocketAddress;
import cn.b2b.common.search.bean.ProductHits;
import cn.b2b.common.search.bean.QueryParams;
import cn.b2b.common.search.bean.SearchParam;
import cn.b2b.common.search.bean.SearchResult;
import cn.b2b.common.search.bean.company.ClusterBean;
import cn.b2b.common.search.bean.company.ClusterQueue;
import cn.b2b.common.search.bean.product.ProductQueryParam;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.util.Constants;
import cn.b2b.common.search.util.Stopwords;
import cn.b2b.index.product.common.ProductConstants;

public class ProductSearcher extends Searcher {

    public static final Hits NULL = new Hits(0, new Hit[0]);

    public ProductSearcher(String config) throws IOException {
        super(config);
        try {
            Stopwords.loadClassPath();
        } catch (Exception e) {
            LOG.info("load stopwords exception", e);
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.easou.client.search.client.Searcher#getDetail(com.easou.common.rpc .io.Writable, java.lang.String[])
     */
    @Override
    public Writable getDetail(int indexNo, Writable searchParam) throws IOException, InterruptedException {
        IndexInetSocketAddress[] indexs = indexServer.get(indexNo);
        InetSocketAddress addr = indexServer.select(indexs);
        if (addr == null) {
            return null;
        }
        Writable obj = indexServerClient.call(searchParam, addr);
        if (obj != null) {
            return ((SearchResult) obj).getValue();
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.easou.client.search.client.Searcher#getDetail(com.easou.common.rpc .io.Writable, java.lang.String[])
     */
    @Override
    public Writable[] getDetails(int[] indexNos, Writable[] params) throws IOException, InterruptedException {
        InetSocketAddress[] addrs = new InetSocketAddress[indexNos.length];
        for (int i = 0; i < indexNos.length; i++) {
            IndexInetSocketAddress[] indexs = indexServer.get(indexNos[i]);
            InetSocketAddress addr = indexServer.select(indexs);
            if (addr == null) {
                return null;
            }
            addrs[i] = addr;
        }
        Writable[] writables = indexServerClient.call(params, addrs);
        Writable[] results = new HitDetails[writables.length];
        for (int i = 0; i < results.length; i++) {
            if (writables[i] == null) {
                continue;
            }
            results[i] = ((SearchResult) writables[i]).getValue();
        }
        return results;
    }

    @Override
    public Writable search(QueryParams param) throws Exception {
        InetSocketAddress[] addrs = indexServer.select();
        
        SearchParam[] params = new SearchParam[addrs.length];
        SearchParam searchParam = new SearchParam(Constants.OP_PRODUCT_SEARCH, param);
        for (int i = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }
        Writable[] objs = indexServerClient.call(params, addrs);
        SearchResult[] ret = new SearchResult[objs.length];
        for (int i = 0; i < objs.length; i++) {
            ret[i] = (SearchResult) objs[i];
        }
        if (((ProductQueryParam)param).getSearchType() == ProductConstants.SEARCH_TYPE_TRADE || ((ProductQueryParam)param).getSearchType() == ProductConstants.SEARCH_TYPE_TRADE_CLUST) {
            return wrapInTimeRet(ret, addrs);
        }
        return wrapRet(ret, addrs);
    }

    @Override
    protected Writable wrapRet(SearchResult[] results, InetSocketAddress[] addrs) {
        if (results.length == 0) {
            return NULL;
        }
        List<Hit> histList = new ArrayList<Hit>();
        int totalHits = 0;
        int totalGrpHits = 0;
        Map<Integer, Integer> tradeMap = new HashMap<Integer, Integer>();
        Map<Integer, Integer> industryMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < results.length; i++) {
            if (results[i] == null) {
                continue;
            }
            ProductHits hits = (ProductHits) results[i].getValue();
            if (hits == null) {
                continue;
            }
            ClusterBean[] tradeids = hits.getTradeids();
            for (int k = 0; k < tradeids.length; k++) {
                if (tradeMap.get(tradeids[k].getId()) != null) {
                    int num = tradeMap.get(tradeids[k].getId()) + tradeids[k].getNum();
                    tradeMap.put(tradeids[k].getId(), num);
                } else {
                    tradeMap.put(tradeids[k].getId(), tradeids[k].getNum());
                }
            }
            ClusterBean[] industryids = hits.getIndustryids();
            for (int k = 0; k < industryids.length; k++) {
                if (industryMap.get(industryids[k].getId()) != null) {
                    int num = industryMap.get(industryids[k].getId()) + industryids[k].getNum();
                    industryMap.put(industryids[k].getId(), num);
                } else {
                    industryMap.put(industryids[k].getId(), industryids[k].getNum());
                }
            }
            for (Hit hit : hits.getHits()) {
                hit.setIndexServerNo((short) this.indexServer.getIndexOfAddress(addrs[i]));
                histList.add(hit);
            }
            totalHits = totalHits + hits.getTotal();
            totalGrpHits = totalGrpHits + hits.getTotalGrp();
        }
        LOG.info("SIZE:" + tradeMap.size() + "\t" + industryMap.size());
        Set<Integer> ckeySet = tradeMap.keySet();
        Iterator<Integer> citer = ckeySet.iterator();
        ClusterQueue tradeQueue = new ClusterQueue(Constants.DEFAULT_CLUSTER_SIZE);
        int minNum = 0;
        while (citer.hasNext()) {
            int key = citer.next();
            int value = tradeMap.get(key);
            if (tradeQueue.size() < Constants.DEFAULT_CLUSTER_SIZE || value > minNum) {
                ClusterBean bean = new ClusterBean(key, value);
                tradeQueue.insert(bean);
                minNum = ((ClusterBean)tradeQueue.top()).getNum();
            }
        }
        citer = null;
        tradeMap.clear();
        tradeMap = null;
        
        Set<Integer> ikeySet = industryMap.keySet();
        Iterator<Integer> iiter = ikeySet.iterator();
        ClusterQueue industryQueue = new ClusterQueue(Constants.DEFAULT_CLUSTER_SIZE);
        minNum = 0;
        while (iiter.hasNext()) {
            int key = iiter.next();
            int value = industryMap.get(key);
            if (industryQueue.size() < Constants.DEFAULT_CLUSTER_SIZE || value > minNum) {
                ClusterBean bean = new ClusterBean(key, value);
                industryQueue.insert(bean);
                minNum = ((ClusterBean)industryQueue.top()).getNum();
            }
        }
        iiter = null;
        industryMap.clear();
        industryMap = null;
        
        ClusterBean[] categoryids = new ClusterBean[tradeQueue.size()];
        for (int i = tradeQueue.size() - 1; i >= 0; i--) {
        	categoryids[i] = ((ClusterBean)tradeQueue.pop());
        	LOG.info("TRADEID:" + categoryids[i].getId() + "\t" + categoryids[i].getNum());
        }
        ClusterBean[] industryids = new ClusterBean[industryQueue.size()];
        for (int i = industryQueue.size() - 1; i >= 0; i--) {
            industryids[i] = ((ClusterBean)industryQueue.pop());
            LOG.info("INDUSTRYID:" + industryids[i].getId() + "\t" + industryids[i].getNum());
        }
        Collections.sort(histList,Hit.comparator);
        /*
        Collections.sort(histList, new Comparator<Hit>()
        		{

            public int compare(Hit o1, Hit o2) {
                float sub = o2.getScore() - o1.getScore();
                if (sub > 0) {
                    return 1;
                } else if (sub < 0) {
                    return -1;
                } else {
                    // use new doc
                    return o2.getIndexDocNo() - o1.getIndexDocNo();
                }
            }
            
        });
        */
        return new ProductHits(totalHits, totalGrpHits, histList.toArray(new Hit[0]), categoryids, industryids);
    }

    protected Writable wrapInTimeRet(SearchResult[] results, InetSocketAddress[] addrs) {
        Calendar cal = Calendar.getInstance();
        long current = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long amonthago = cal.getTimeInMillis();
        if (results.length == 0) {
            return NULL;
        }
        List<Hit> histList = new ArrayList<Hit>();
        int totalHits = 0;
        int totalGrpHits = 0;
        for (int i = 0; i < results.length; i++) {
            if (results[i] == null) {
                continue;
            }
            ProductHits hits = (ProductHits) results[i].getValue();
            if (hits == null) {
                continue;
            }
            
            for (Hit hit : hits.getHits()) {
                hit.setIndexServerNo((short) this.indexServer.getIndexOfAddress(addrs[i]));
                if (hit.getIntime() > amonthago) {
                    hit.setScore(hit.getIntime());
                } else {
                    hit.setScore(current + hit.getScore());
                }
                histList.add(hit);
            }
            
            totalHits = totalHits + hits.getTotal();
            totalGrpHits = totalGrpHits + hits.getTotalGrp();
        }
        Collections.sort(histList,Hit.comparator);
        /*
        Collections.sort(histList, new Comparator<Hit>() {

            public int compare(Hit o1, Hit o2) {
                float sub = o2.getScore() - o1.getScore();
                if (sub > 0) {
                    return 1;
                } else if (sub < 0) {
                    return -1;
                } else {
                    // use new doc
                    return o2.getIndexDocNo() - o1.getIndexDocNo();
                }
            }
            
        });
        */

        return new ProductHits(totalHits, totalGrpHits, histList.toArray(new Hit[0]));
    }

    public void delete(int[] docs) throws Exception {
        InetSocketAddress[] addrs = indexServer.get(0);
        DocumentDelParam delparam = new DocumentDelParam(docs);
        SearchParam searchParam = new SearchParam(Constants.OP_DELDOCS, delparam);
        for (int i  = 0; i < addrs.length; i++) {
            indexServerClient.call(searchParam, addrs[i]);
        }
    }

    public int delete(Query query, byte datatype, byte memlevel) throws Exception {
        DelDocsByWordBean delBean = new DelDocsByWordBean(query, 0, datatype, memlevel);
        InetSocketAddress[] addrs = indexServer.getAll();

        SearchParam[] params = new SearchParam[addrs.length];
        SearchParam searchParam = new SearchParam(Constants.OP_DELDOCS_BY_WORD, delBean);
        for (int i = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }

        Writable[] objs = indexServerClient.call(params, addrs);
        SearchResult[] rets = new SearchResult[objs.length];
        int result = 0;
        for (int i = 0; i < rets.length; i++) {
            rets[i] = (SearchResult) objs[i];
            result += ((IntWritable)rets[i].getValue()).get();
        }
        return result;
    }

    public boolean updateMemory(int docNum, Map<String, String> updateMemoryData) throws Exception {
        DataUpdateBean updateBean = new DataUpdateBean(docNum, updateMemoryData);
        InetSocketAddress[] addrs = indexServer.getAll();

        SearchParam[] params = new SearchParam[addrs.length];
        SearchParam searchParam = new SearchParam(Constants.OP_UPDATE_MEMORY_DATA, updateBean);
        for (int i = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }
        Writable[] objs = indexServerClient.call(params, addrs);
        SearchResult[] rets = new SearchResult[objs.length];
        boolean result = false;
        for (int i = 0; i < rets.length; i++) {
            rets[i] = (SearchResult) objs[i];
            result = ((BooleanWritable)rets[i].getValue()).get();
            if (result) 
                break;
        }
        return result;
    }

    public Writable getIdByDelWord(Query query, int num, byte datatype, byte memlevel) throws Exception {
        DelDocsByWordBean delBean = new DelDocsByWordBean(query, num, datatype, memlevel);
        InetSocketAddress[] addrs = indexServer.select();
        SearchParam[] params = new SearchParam[addrs.length];
        SearchParam searchParam = new SearchParam(Constants.OP_GET_ID_BY_DEL_WORD, delBean);
        for (int i = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }
        Writable[] objs = indexServerClient.call(params, addrs);

        SearchResult[] ret = new SearchResult[objs.length];
        for (int i = 0; i < objs.length; i++) {
            ret[i] = (SearchResult) objs[i];
        }
        return wrapRet(ret, addrs);
    }
    
    public void deleteByProductid(int[] productids) throws Exception {
        InetSocketAddress[] addrs = indexServer.getAll();
        SearchParam[] params = new SearchParam[addrs.length];
        DocumentDelParam delparam = new DocumentDelParam(productids);
        SearchParam searchParam = new SearchParam(Constants.OP_DEL_BY_USER_ID, delparam);
        for (int i  = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }
        indexServerClient.call(params, addrs);
        
    }
    
    public void unDeleteByUserid(int userid) throws Exception {
        InetSocketAddress[] addrs = indexServer.getAll();
        SearchParam[] params = new SearchParam[addrs.length];
        SearchParam searchParam = new SearchParam(Constants.OP_UNDELETE_DOC, new IntWritable(userid));
        for (int i  = 0; i < addrs.length; i++) {
            params[i] = searchParam;
        }
        indexServerClient.call(params, addrs);
    }
}

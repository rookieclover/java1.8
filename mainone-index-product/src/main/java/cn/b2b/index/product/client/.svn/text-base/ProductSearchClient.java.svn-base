package cn.b2b.index.product.client;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import cn.b2b.common.rpc.io.Writable;
import cn.b2b.common.search.bean.DetailParam;
import cn.b2b.common.search.bean.Hit;
import cn.b2b.common.search.bean.HitDetails;
import cn.b2b.common.search.bean.Hits;
import cn.b2b.common.search.bean.ProductHits;
import cn.b2b.common.search.bean.SearchParam;
import cn.b2b.common.search.bean.company.ClusterBean;
import cn.b2b.common.search.bean.product.ProductQueryParam;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.SearchQuery;
import cn.b2b.common.search.util.Constants;
import cn.b2b.index.product.common.ProductConstants;

/**
 * 小说搜索客户端.
 * 
 * @author yunchat
 * 
 */
public class ProductSearchClient {
    private Logger LOG = Logger.getLogger("search");
    private ProductSearcher searcher;

    /**
     * @param config
     *            client.properties 配置文件绝对路径。
     * @throws IOException
     */
    public ProductSearchClient(String config) throws IOException {
        searcher = new ProductSearcher(config);
    }

    /**
     * @param searchItem
     *            查询条件
     * @param searchType
     *            1 查询和聚合 2 仅查询 3 仅聚合 4 仅查 行业分类
     * @param sortItem
     *            0 相关性 1 会员级别 2 相关性 3 注册时间
     * @return
     * @throws Exception
     */
    public ProductResultBean search(ISearchProductItem searchItem) throws Exception  {
    	
    	DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    	
        
        ProductResultBean result = new ProductResultBean();
        String query = "";
        if (searchItem.getKeyWord() != null && searchItem.getKeyWord().trim().length() > 0) {
            query += searchItem.getKeyWord();
        }
//        if (searchItem.getNotKeyWord() != null && searchItem.getNotKeyWord().trim() != "") {
//            query += " - " + searchItem.getNotKeyWord();
//        }
        if (searchItem.getTradeID() != -1) {
            query += " tradeid:" + searchItem.getTradeID();
        }
        if (searchItem.getIndustryID() != -1) {
            query += " industryid:" + searchItem.getIndustryID();
        }
        if (searchItem.getBrand() != null) {
            query += " brand:" + searchItem.getBrand();
        }
        LOG.info("Query keyword is \t:" +searchItem.getKeyWord() );
        Query qQuery = SearchQuery.parse(query, Constants.IDF_MEMCACHE_TYPE_PRODUCT,null,1);
        ProductQueryParam param = new ProductQueryParam(qQuery);
        Query notQuery = new Query();
        if (searchItem.getNotKeyWord() != null && searchItem.getNotKeyWord().trim().length() > 0) {
            notQuery = SearchQuery.parse("anchor:" + searchItem.getNotKeyWord(), Constants.IDF_MEMCACHE_TYPE_PRODUCT);
        }
        param.setNotQuery(notQuery);
        Query attrQuery = new Query();
        if (searchItem.getAttrKeyWord() != null && searchItem.getAttrKeyWord().trim().length() > 0) {
            attrQuery = SearchQuery.parse(searchItem.getAttrKeyWord(), Constants.IDF_MEMCACHE_TYPE_PRODUCT);
        }
        param.setAttrQuery(attrQuery);
        param.setHitsNum((searchItem.getPageindex() + 1) * searchItem.getPagesize());
        param.setSortType(searchItem.getSortitems());
        param.setSearchType(searchItem.getSearchType());
        if (searchItem.getSearchType() == ProductConstants.SEARCH_TYPE_TRADE || searchItem.getSearchType() == ProductConstants.SEARCH_TYPE_TRADE_CLUST) {
            param.setSortType(ProductConstants.SORT_TYPE_IN_TIME);
        } else {
            param.setSortType(searchItem.getSortitems());
        }
        param.setArea(searchItem.getArea());
        
        param.setProvince(searchItem.getProvince());
        param.setCity(searchItem.getCity());
//        param.setBrand(searchItem.getBrand());
        param.setMemberLevel(searchItem.getMemberLevel());
        param.setCompanyLicense((byte)searchItem.getCompanyLicense());
        param.setDatatype(searchItem.getDatatype());
        param.setHaspic(searchItem.getHaspic());
        param.setTradeLevel(searchItem.getTradeLevel());
        long start = System.currentTimeMillis();
        
        long end = System.currentTimeMillis();

        LOG.info("QueryParam:" +param );

        ProductHits hits = (ProductHits) searcher.search(param);
           
        LOG.info("QUERY:" + qQuery + "\tSEARCH_TYPE:" + searchItem.getSearchType() + "\tAREA:" + 
                searchItem.getArea() + "\tCITY:" + searchItem.getCity() + "\tProvince:" + searchItem.getProvince() +
                "\tATTR:" + searchItem.getAttrKeyWord() + "\tBRAND:" + searchItem.getBrand() + "\tLICENSE:" + 
                searchItem.getCompanyLicense() + "\tDATATYPE:" + searchItem.getDatatype() + "\tPIC:" + searchItem.getHaspic() +
                "\tINDUSTRYID:" + searchItem.getIndustryID() + "\tTRADEID:" + searchItem.getTradeID() +"\tMELEVEL:" +
                searchItem.getMemberLevel() + "\tNOTKEY:" + searchItem.getNotKeyWord() + "\t" + searchItem.getPageindex() + "\t" + searchItem.getPagesize() +
                "\tSORT:" + searchItem.getSortitems() + "\tPRODUCT RESULT SIZE:" + hits.getTotal() + "\t" + hits.getTotalGrp() + "\tTIME:" + (end - start));
        
        if (hits != null) {
            List<ITuple> tradeList = new ArrayList<ITuple>();
            ClusterBean[] categoryids = hits.getTradeids();

            for (int i = 0; i < categoryids.length; i++) {
                tradeList.add(new ITuple(categoryids[i].getId(), categoryids[i].getNum()));
            }
            result.setTradeids(tradeList);
            List<ITuple> industryList = new ArrayList<ITuple>();
            ClusterBean[] industryids = hits.getIndustryids();
            for (int i = 0; i < industryids.length; i++) {
                industryList.add(new ITuple(industryids[i].getId(), industryids[i].getNum()));
            }
            result.setIndustryids(industryList);
            result.setCount(hits.getTotal());
            result.setClustCount(hits.getTotalGrp());
            int pageindex = searchItem.getPageindex();
            int pagesize = searchItem.getPagesize();
            int hitesnum = pagesize;
            if (hits.getLength() < (pageindex + 1) * pagesize) {
                hitesnum = hits.getLength() - (pageindex * pagesize);
            }
            if (hitesnum < 0) {
            	List<IProductSearchInfo> searchResult = new ArrayList<IProductSearchInfo>();
            	result.setSearchResultList(searchResult);
            	return result;
            }
            Hit[] hites = new Hit[hitesnum];
            int idx = 0;
            for (int i = pageindex * pagesize; i < (pageindex + 1) * pagesize && i < hits.getLength(); i++) {
                hites[idx++] = hits.getHits()[i];
            }
            List<IProductSearchInfo> searchResult = new ArrayList<IProductSearchInfo>();
            HitDetails[] details = getDetails(hites, qQuery.getQueryStr());

            for (int i = 0; i < details.length; i++) {
            	if (details[i] == null) continue;
                IProductSearchInfo searchInfo = new ProductSearchInfo();

                searchInfo.setProductID(Integer.parseInt(details[i].getValue("docnum")));

                searchInfo.setProductTitle(details[i].getValue("title"));

                searchInfo.setProductDescription(details[i].getValue("content"));

                searchInfo.setProductImgPath(details[i].getValue("pic"));

                searchInfo.setHasCompanyLicense(Integer.parseInt(details[i].getValue("license")));

                searchInfo.setHasDbsh(0);

                searchInfo.setMemberLevel(Integer.parseInt(details[i].getValue("memlevel")));

                searchInfo.setInternal(details[i].getValue("datatype").equals("2") ? true : false);

                searchInfo.setCompanyName(details[i].getValue("cname"));

                searchInfo.setCompanyUrl(details[i].getValue("curl"));

                searchInfo.setProductUrl(details[i].getValue("url"));

                searchInfo.setCompanyId(Integer.parseInt(details[i].getValue("cid")));
                searchInfo.setUserId(Integer.parseInt(details[i].getValue("userid")));
                String[] tradeids = details[i].getValue("tradeid").split(" ");
                searchInfo.setCategoryId(Integer.parseInt(tradeids[0]));

                searchInfo.setIsTradeGlobal(0);

                searchInfo.setKeywords(details[i].getValue("keyword"));
                String brand = details[i].getValue("brand");
                searchInfo.setBrand(brand);

                searchInfo.setCompanyUrl(details[i].getValue(""));
                searchInfo.setPrice(details[i].getValue("price"));
                searchInfo.setCompanyAddress(details[i].getValue("caddress"));
                searchInfo.setCompanyScale(details[i].getValue("size"));
                searchInfo.setProductAddress("");
                
                String dateStr = details[i].getValue("updatedate");
                if(dateStr!=null && dateStr.trim().length()>0)
                {
                	try {
						searchInfo.setUpdateDate(dateFormat.parse(dateStr));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						System.out.println("["+ dateStr+"] is updatedate\tpk is:\t" + searchInfo.getProductID() + "\tuserid is " + searchInfo.getUserId() );
						e.printStackTrace();
					}
                }
                
                searchInfo.setSource("铭万网");
                searchInfo.setSourceUrl("http://www.b2b.cn");
                searchInfo.setProvince(Integer.parseInt(details[i].getValue("province")));
                searchInfo.setCity(Integer.parseInt(details[i].getValue("city")));
                //prop.getProperty("spec")+"\t" +prop.getProperty("unit")+"\t" +prop.getProperty("mincount")
                searchInfo.setSpec(details[i].getValue("spec"));
                searchInfo.setUnit(details[i].getValue("unit"));
                searchInfo.setMincount(details[i].getValue("mincount"));
                searchInfo.setScore(hites[i].getScore());
                searchResult.add(searchInfo);
            }
            result.setSearchResultList(searchResult);
        } else {
            result.setCount(0);
        }
        return result;
    }

    //
    // /**
    // * @param query 搜索词
    // * @param num 搜索数量
    // * @param searchType 搜索类型
    // * @param sortType 打分类型
    // * @param groupType 0:不折叠 1:按gid折叠
    // * @param sieve 推荐 Sieve.Yes 推荐1，Sieve.No:0.
    // * @return
    // * @throws Exception
    // */
    // public Hits search(SearchItem searchItem) throws Exception {
    // String query = searchItem.getKeyWord();
    // if (searchItem.getNotKeyWord() != null &&
    // searchItem.getNotKeyWord().trim() != "") {
    // query += " - " + searchItem.getNotKeyWord();
    // }
    // if (searchItem.getCategoryID() != -1) {
    // query += " categoryid:" + searchItem.getCategoryID();
    // }
    // if (searchItem.getIndustryID() != -1) {
    // query += " industryid:" + searchItem.getIndustryID();
    // }
    // Query qQuery = SearchQuery.parse(query);
    // System.out.println(qQuery);
    // return (Hits) searcher.search(new CompanyQueryParam(qQuery, searchItem));
    // }
    /**
     * @param query
     *            搜索词
     * @param num
     *            搜索数量
     * @param searchType
     *            搜索类型 1 anchor 书名搜索
     * @param sortType
     *            打分类型
     * @param groupType
     *            0:不折叠 1:按gid折叠
     * @param sieve
     *            推荐 Sieve.Yes 推荐1，Sieve.No:0.
     * @return
     * @throws Exception
     */
    public Hits search(Query qQuery, ISearchProductItem searchItem) throws Exception {
        // String query = searchItem.getKeyWord();
        // if (searchItem.getNotKeyWord() != null &&
        // searchItem.getNotKeyWord().trim() != "") {
        // query += " - " + searchItem.getNotKeyWord();
        // }
        // if (searchItem.getCategoryID() != -1) {
        // query += " categoryid:" + searchItem.getCategoryID();
        // }
        // if (searchItem.getIndustryID() != -1) {
        // query += " industryid:" + searchItem.getIndustryID();
        // }
        // Query qQuery = SearchQuery.parse(query);
        // System.out.println(qQuery);
        ProductQueryParam param = new ProductQueryParam(qQuery);
        if (searchItem.getAttrKeyWord() != null) {
            Query attrQuery = SearchQuery.parse(searchItem.getAttrKeyWord(), Constants.IDF_MEMCACHE_TYPE_PRODUCT);
            param.setAttrQuery(attrQuery);
        }
        param.setHitsNum(searchItem.getPageindex() * searchItem.getPagesize());
        param.setSortType(searchItem.getSortitems());
        param.setSearchType(searchItem.getSearchType());
        param.setArea(searchItem.getArea());
        param.setProvince(searchItem.getProvince());
        param.setCity(searchItem.getCity());
        param.setBrand(searchItem.getBrand());
        param.setMemberLevel(searchItem.getMemberLevel());
        param.setCompanyLicense((byte)searchItem.getCompanyLicense());
        param.setDatatype(searchItem.getDatatype());
        param.setHaspic(searchItem.getHaspic());
        return (Hits) searcher.search(param);
    }

    /**
     * 获取小说详细
     * 
     * @param hit
     *            小说hit
     * @return 小说详细
     * @throws Exception
     */
    public HitDetails getDetail(Hit hit, String query) throws Exception {
        SearchParam param = new SearchParam(Constants.OP_DETAIL, new DetailParam(hit.getIndexDocNo(), query, Constants.SUMMARY_TYPE_SUMMARY));
        Writable ws = searcher.getDetail(hit.getIndexServerNo(), param);
        if (ws == null) {
            return null;
        }
        return (HitDetails) ws;
    }

    /**
     * 获取小说详细
     * 
     * @param hits
     *            小说hits
     * @return
     * @throws Exception
     */
    public HitDetails[] getDetails(Hit[] hits, String query) throws Exception {
        Writable[] params = new Writable[hits.length];
        int[] indexNos = new int[hits.length];
        for (int i = 0; i < hits.length; i++) {
            params[i] = new SearchParam(Constants.OP_DETAIL, new DetailParam(hits[i].getIndexDocNo(), query, Constants.SUMMARY_TYPE_SUMMARY));
            indexNos[i] = hits[i].getIndexServerNo();
        }
        return (HitDetails[]) searcher.getDetails(indexNos, params);
    }

    /**
     * 更新内存数据
     * @param updateBean
     * @throws Exception
     */
    public boolean update(IProductUpdateBean updateBean) throws Exception {
    	DateFormat format = new SimpleDateFormat("yyyyMMdd");
    	
        LOG.info("UPDATE:" + updateBean.toString());
        Map<String, String> updateMemoryData = new HashMap<String, String>();
        if (updateBean.getTradeid() != null && updateBean.getTradeid().length() > 0) {
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
        if (updateBean.getInsertdate() != null) {
            updateMemoryData.put("idate", updateBean.getInsertdate().getTime() + "");
        }
        if (updateBean.getOuttime() != null) {
            updateMemoryData.put("otime", updateBean.getOuttime().getTime() + "");
        }
        if (updateBean.getUpdatedate() != null) {
            updateMemoryData.put("udate", updateBean.getUpdatedate().getTime() + "");
            updateMemoryData.put("utime", format.parse(format.format(updateBean.getUpdatedate())).getTime() + "");
        }
        if (updateBean.getState() != -1) {
            updateMemoryData.put("state", updateBean.getState() + "");
        }
        return searcher.updateMemory(updateBean.getProductid(), updateMemoryData);
    }
    /**
     * 根据用户ID进行删除
     * @param productids
     * @throws Exception
     */
    public void deleteByProductid(int[] productids) throws Exception {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < productids.length; i++) {
            buf.append(productids[i] + " ");
        }
        LOG.info("DEL:" + buf.toString());
        searcher.deleteByProductid(productids);
    }
    /**
     * 根据词进行删除
     * @param word
     * @throws Exception
     */
    public int deleteByWord(String word, int num, byte datatype, byte memlevel) throws Exception {
        Query query = SearchQuery.parse(word, Constants.IDF_MEMCACHE_TYPE_PRODUCT);
        return searcher.delete(query, datatype, memlevel);
    }
    
    public void unDeleteByUserid(int userid) throws Exception {
        searcher.unDeleteByUserid(userid);
    }
    

//    /**
//     * 内部使用，更新某一字段内容
//     * @param docNum
//     * @param field
//     * @param value
//     * @throws Exception
//     */
//    public void updateMemoryData(int docNum, String field, String value) throws Exception {
//        searcher.updateMemoryData(docNum, field, value);
//    }
 
    /**
     * 根据词，取得需删除的公司ID。
     * @param word
     * @return
     * @throws Exception
     */
    public List<IProductSearchInfo> getDelWordCompanyID(String word, int num, byte datatype, byte memlevel) throws Exception {
    	
    	DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        Query query = SearchQuery.parse(word, Constants.IDF_MEMCACHE_TYPE_PRODUCT);
        Hits hits = (Hits)searcher.getIdByDelWord(query, num, datatype, memlevel);
        List<IProductSearchInfo> searchResult = new ArrayList<IProductSearchInfo>();
        Hit[] hites = new Hit[hits.getLength()];
        HitDetails[] details = getDetails(hites, "");
//        String[] explains = getExplain(hites, qQuery);
        for (int i = 0; i < details.length; i++) {
            IProductSearchInfo searchInfo = new ProductSearchInfo();
            searchInfo.setProductID(Integer.parseInt(details[i].getValue("docnum")));

            searchInfo.setProductTitle(details[i].getValue("title"));

            searchInfo.setProductDescription(details[i].getValue("content"));

            searchInfo.setProductImgPath(details[i].getValue("pic"));

            searchInfo.setHasCompanyLicense(Integer.parseInt(details[i].getValue("license")));

            searchInfo.setHasDbsh(0);

            searchInfo.setMemberLevel(Integer.parseInt(details[i].getValue("memlevel")));

            searchInfo.setInternal(details[i].getValue("datatype").equals("2") ? true : false);

            searchInfo.setCompanyName(details[i].getValue("cname"));

            searchInfo.setCompanyUrl(details[i].getValue("curl"));

            searchInfo.setProductUrl(details[i].getValue("url"));

            searchInfo.setCompanyId(Integer.parseInt(details[i].getValue("cid")));
            String[] tradeids = details[i].getValue("tradeid").split(" ");
            searchInfo.setCategoryId(Integer.parseInt(tradeids[0]));

            searchInfo.setIsTradeGlobal(0);

            searchInfo.setKeywords(details[i].getValue("keyword"));
            String brand = details[i].getValue("brand");
            searchInfo.setBrand(brand);

            searchInfo.setCompanyUrl(details[i].getValue(""));
            searchInfo.setPrice(details[i].getValue("price"));
            searchInfo.setCompanyAddress(details[i].getValue("caddress"));
            searchInfo.setCompanyScale(details[i].getValue("size"));
            searchInfo.setProductAddress("");

            searchInfo.setUpdateDate(dateFormat.parse(details[i].getValue("updatedate")));
            searchInfo.setSource("铭万网");
            searchInfo.setSourceUrl("http://www.b2b.cn");

            searchInfo.setProvince(Integer.parseInt(details[i].getValue("province")));

            searchInfo.setCity(Integer.parseInt(details[i].getValue("city")));

            searchInfo.setScore(hites[i].getScore());

            searchResult.add(searchInfo);
        }
        return searchResult;
    }
    
    public static void main(String[] args) throws Exception {
        ProductSearchClient client = new ProductSearchClient(args[0]);
        // SearchItem searchItem = new SearchItem();

        // client.search(null, searchItem);
    }
}

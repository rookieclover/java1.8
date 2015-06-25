import java.util.ArrayList;
import java.util.List;

import cn.b2b.common.rpc.io.Writable;
import cn.b2b.common.search.bean.DetailParam;
import cn.b2b.common.search.bean.Hit;
import cn.b2b.common.search.bean.HitDetails;
import cn.b2b.common.search.bean.Hits;
import cn.b2b.common.search.bean.SearchParam;
import cn.b2b.common.search.bean.company.CompanyQueryParam;
import cn.b2b.common.search.bean.product.ProductQueryParam;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.SearchQuery;
import cn.b2b.common.search.util.Constants;
import cn.b2b.index.product.client.IProductSearchInfo;
import cn.b2b.index.product.client.ProductResultBean;
import cn.b2b.index.product.client.ProductSearcher;
import cn.b2b.index.product.common.ProductConstants;

public class ProductMonitorSearch {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ProductSearcher searcher = new ProductSearcher(args[0]);
        String query = args[1];
        Query qQuery = SearchQuery.parse(query, Constants.IDF_MEMCACHE_TYPE_COMPANY);
        ProductQueryParam param = new ProductQueryParam(qQuery);
        Query notQuery = new Query();
        param.setNotQuery(notQuery);
        Query attrQuery = new Query();
        param.setAttrQuery(attrQuery);
        param.setHitsNum(30);
        param.setSearchType(ProductConstants.SEARCH_TYPE_ONLY_SEARCH);
        param.setSortType(ProductConstants.SORT_TYPE_DEFAULT);
        param.setArea(-1);
        param.setProvince(-1);
        param.setCity(-1);
        param.setMemberLevel(-1);
        param.setDatatype((byte) 0);
        param.setTradeLevel(1);
        param.setCompanyLicense((byte)-1);
        param.setHaspic((byte)-1);
        param.setTradeLevel(1);
        try {
            ProductResultBean result = new ProductResultBean();
        
            Hits hits = (Hits) searcher.search(param);
            if (hits != null) {
                
                result.setCount(hits.getTotal());
                if (hits.getLength() > 0) {
                    Hit[] hites = new Hit[hits.getLength()];
                    int idx = 0;
                    Hit[] hitss = hits.getHits();
                    for (int i = 0; i < hits.getLength(); i++) {
                        hites[idx++] = hitss[i];
                    }
                    
                    List<IProductSearchInfo> searchResult = new ArrayList<IProductSearchInfo>();
                    Writable[] params = new Writable[hites.length];
                    int[] indexNos = new int[hites.length];
                    for (int i = 0; i < hites.length; i++) {
                        params[i] = new SearchParam(Constants.OP_DETAIL, new DetailParam(hites[i].getIndexDocNo(), query, Constants.SUMMARY_TYPE_SUMMARY));
                        indexNos[i] = hites[i].getIndexServerNo();
                    }
                    HitDetails[] details =  (HitDetails[])searcher.getDetails(indexNos, params);
                }
            }
            System.out.println("result:" + hits.getLength() + "\t" + hits.getTotal());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

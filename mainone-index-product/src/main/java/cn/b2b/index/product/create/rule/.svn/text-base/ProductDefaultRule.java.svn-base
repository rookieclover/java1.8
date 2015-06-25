package cn.b2b.index.product.create.rule;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import cn.b2b.common.search.query.DataBean;
import cn.b2b.common.search.query.MyClause;
import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.SearchQuery;
import cn.b2b.common.search.query.rule.DefaultQueryRule;
import cn.b2b.common.search.query.rule.QueryRule;
import cn.b2b.common.search.segment.SegmentManager;
import cn.b2b.common.search.util.Constants;
import cn.b2b.common.search.util.MD5Hash;

public class ProductDefaultRule extends DefaultQueryRule implements QueryRule {

    public static final Logger LOG = Logger.getLogger("lucene");

    @Override
    public void fillDocument(DataBean dataBean, Document doc,
            String[] indexFields, int[] indexTypes) {
        super.fillDocument(dataBean, doc, indexFields, indexTypes);
    }

    @Override
    public void fillQuery(Query query, String field, String clause,
            MyClause myClause) {
        if (field.equals("title") || field.equals("anchor")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(SearchQuery.segWord(clause, Constants.IDF_MEMCACHE_TYPE_PRODUCT), "anchor",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(SearchQuery.segWord(clause, Constants.IDF_MEMCACHE_TYPE_PRODUCT), "anchor");
            }
        } else if (field.equals("docnum")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(clause, "docnum",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(clause, "docnum");
            }
        } else if (field.equals("tradeid")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(clause, "tradeid",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(clause, "tradeid");
            }
        } else if (field.equals("industryid")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(clause, "industryid",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(clause, "industryid");
            }
        } else if (field.equals("keyword")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(SearchQuery.segWord(clause, Constants.IDF_MEMCACHE_TYPE_PRODUCT), "keyword",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(SearchQuery.segWord(clause, Constants.IDF_MEMCACHE_TYPE_PRODUCT), "keyword");
            }
        } else if (field.equals("brand")) {
            if (!myClause.isProhibited()) {
                query.addRequiredTerm(clause, "brand",
                        myClause.isRequired());
            } else {
                query.addProhibitedTerm(clause, "brand");
            }
        }
    }
}

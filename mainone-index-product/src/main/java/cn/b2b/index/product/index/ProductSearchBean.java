package cn.b2b.index.product.index;

import java.io.IOException;

import cn.b2b.common.search.IndexConfig;
import cn.b2b.indexer.search.search.SearchBean;
import cn.b2b.lucene.search.plugin.QueryOptimizer;

public class ProductSearchBean extends SearchBean {

    public ProductSearchBean(IndexConfig config, QueryOptimizer optimizer)
            throws IOException {
        super(config, optimizer);
    }

}

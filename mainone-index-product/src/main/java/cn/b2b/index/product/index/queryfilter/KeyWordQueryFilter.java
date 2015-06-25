package cn.b2b.index.product.index.queryfilter;

import cn.b2b.common.search.query.filter.FieldQueryFilter;

public class KeyWordQueryFilter extends FieldQueryFilter {

    public KeyWordQueryFilter() {
        super("keyword", 1.0f);
    }

}

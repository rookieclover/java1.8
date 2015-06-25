package cn.b2b.index.product.common;

public class ProductConstants {
    public static final byte SORT_TYPE_DEFAULT = 0;
    public static final byte SORT_TYPE_MEM_LEVEL = 1;
    public static final byte SORT_TYPE_SIMILAR= 2;
    public static final byte SORT_TYPE_IN_TIME = 3;
    
    public static final byte SEARCH_TYPE_ALL = 1;
    public static final byte SEARCH_TYPE_ONLY_SEARCH = 2;
    public static final byte SEARCH_TYPE_ONLY_CLUSTER = 3;
    public static final byte SEARCH_TYPE_TRADE = 4;
    
    public static final byte SEARCH_TYPE_ALL_CLUST = 5;
    public static final byte SEARCH_TYPE_ONLY_SEARCH_CLUST = 6;
    public static final byte SEARCH_TYPE_TRADE_CLUST = 7;
    
    public static final byte QUERY_TYPE_MUST_OR = 1;
    
    public static final byte QUERY_TYPE_MUST_AND = 2;
    public static final byte QUERY_TYPE_MUST_AND_MUST_HAS_NEED_WORD = 4;

}

import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BitVector;

import cn.b2b.common.search.IndexConfig;
import cn.b2b.common.search.query.filter.QueryFilters;
import cn.b2b.lucene.search.memory.MemoryFieldCache;


public class TestRead {

    public static void main(String[] args) throws Exception {
        IndexReader reader2 = IndexReader.open("E:\\data\\aaa\\product_db_intime\\index");
        
        System.out.println(reader2.maxDoc());
        IndexConfig config = new IndexConfig();
        config.initConfig("E:\\Work\\Java\\Service\\mainone-index-product\\config\\index\\index.conf");
        QueryFilters.loadFilters("E:\\Work\\Java\\Service\\mainone-index-product\\config\\index\\filter");

        IndexSearcher searcher = new IndexSearcher(reader2, false, config, null, null);
        for (int i = 0; i < reader2.maxDoc(); i++) {
            Properties prop = searcher.getDetailSummary(i, "", 1, 0);
            BitVector states = (BitVector)MemoryFieldCache.get("haspic");
            System.out.println(prop.getProperty("title") + "\t" + states.get(i));
        }
    }
}

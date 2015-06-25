package cn.b2b.index.product.index;

import org.apache.lucene.store.NIOFSDirectory;

import cn.b2b.common.search.IndexConfig;
import cn.b2b.common.search.query.filter.QueryFilters;
import cn.b2b.common.search.query.rule.QueryRuleManager;
import cn.b2b.common.search.util.Stopwords;
import cn.b2b.indexer.search.indexer.IndexerServer;
import cn.b2b.indexer.search.search.SearchBean;
import cn.b2b.lucene.search.plugin.QueryOptimizer;

public class StartProductIndexServer {

    public static int serverPort = 0;

    public static void main(String[] args) throws Exception {
        String usage = "StartCompanyIndexServer port configFile";

        if (args.length < 2) {
            System.err.println(usage);
            System.exit(-1);
        }
        // /启用NIOFSDirectory，非阻塞读硬盘数据
        System.setProperty("org.apache.lucene.FSDirectory.class",
                NIOFSDirectory.class.getName());

        IndexConfig config = new IndexConfig();
        config.initConfig(args[1]);
        config.setAddressPort(Integer.parseInt(args[0]));

        Stopwords.loadStopwords("stopwords.txt");
        Stopwords.loadSymbol("symbol.txt");
        Stopwords.loadHighFreq("highfreq.txt");
        QueryOptimizer optimizer = new ProductQueryOptimizer(16, 0.05f);
        SearchBean searchBean = new ProductSearchBean(config, optimizer);

        // 初始化查询规则。
        QueryRuleManager.getRulePlugin(config.getQueryRuleClass());
        
        // 初始化查询语句过滤规则。
        QueryFilters.loadFilters(config.getQueryFilterConfigFileName());

        IndexerServer server = new IndexerServer(config.getAddressPort(),
                config.getHandlerNum(), searchBean);
        server.start();

        // searchBean.start();
        // searchBean.join();

        server.join();

    }

}

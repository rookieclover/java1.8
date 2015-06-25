package cn.b2b.index.product.index;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.score.ScorePlugin;
import org.apache.lucene.score.ScorePluginLoader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.PriorityQueue;

import cn.b2b.common.search.bean.company.ClusterHitQueue;
import cn.b2b.index.product.common.ProductConstants;
import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.IndexScorer;
import cn.b2b.lucene.search.plugin.ScoreParam;

public class ProductIndexScorer extends IndexScorer {

    @Override
    public PriorityQueue fieldNoScore(Query query, int nDocs, int[] totalHits,
            BitVector combBit, ScoreParam scoreParam, IndexReader reader)
            throws IOException {

        BitSet results = new BitSet(reader.maxDoc());

        int total = 0;

        PriorityQueue hqueue = new HitQueue(nDocs);
        for (int i = 0; i < results.length(); i++) {
            if (results.get(i)
                    && (combBit == null || (combBit != null && combBit.get(i)))) {
                total++;
                hqueue.insert(new FieldDoc(i, 1.0f));
            }
        }
        totalHits[0] = total;
        return hqueue;
    }

    @Override
    public PriorityQueue scoreHighFreq(Query query, int nDocs,
            BitVector combBit, ScoreParam scoreParam, IndexReader reader)
            throws Exception {
        boolean bContinue = true;
        final PriorityQueue hitq = new HitQueue(nDocs);
        for (int i = combBit.size() - 1; i > 0 && bContinue; i--) {
            if (combBit.get(i)) {
                float pscore = 1.1f;
                ScoreDoc scoreDoc = new ScoreDoc(i, pscore);

                bContinue = hitq.insert(scoreDoc);
            }
        }
        return hitq;
    }

    @Override
    public ClusterHitQueue score(final Query query, final Scorer scorer,
            final int[] totalHits, final int nDocs, final BitSet bits,
            final BitVector combBit, final ScoreParam scoreParam,
            final IndexReader reader, final BitVector bites) throws Exception {
        final ClusterHitQueue hitq = new ClusterHitQueue(nDocs);

        final ScorePlugin plugin = ScorePluginLoader.getScorePlugin(String
                .valueOf(scoreParam.getSortType()));
        plugin.initCache(reader);
        final int[][] tradeids = (int[][]) MemoryFieldCache.get(reader,
                "tradeid");
        final int[] industryid = (int[]) MemoryFieldCache.get(reader,
                "industryid");
        final int[] userids = (int[]) MemoryFieldCache.get(reader, "userid");

        if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_ALL) {
            scorer.score(new HitCollector() {
                private float minScore = 0.0f;

                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    pscore *=scoreParam.getScorePercent();
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {
                        totalHits[0]++;
                        hitq.cluster(tradeids[doc], industryid[doc],scoreParam.getTradeLevel());
                        if (hitq.size() < nDocs || pscore > minScore) {
                            hitq.insert(new ScoreDoc(doc, score, pscore));
                            minScore = ((ScoreDoc) hitq.top()).pscore;
                        }
                    }
                }
            });
        } else if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_ONLY_SEARCH) {
            scorer.score(new HitCollector() {
                private float minScore = 0.0f;

                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    pscore *=scoreParam.getScorePercent();
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {
                        totalHits[0]++;
                        if (hitq.size() < nDocs || pscore > minScore) {
                            hitq.insert(new ScoreDoc(doc, score, pscore));
                            minScore = ((ScoreDoc) hitq.top()).pscore;
                        }
                    }
                }
            });

        } else if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_TRADE) {
            final long[] updatedates = (long[]) MemoryFieldCache.get(reader,
                    "udate");
            scorer.score(new HitCollector() {
                private float minScore = 0.0f;

                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {
                        totalHits[0]++;
                        hitq.cluster(tradeids[doc], industryid[doc],scoreParam.getTradeLevel());
                        if (hitq.size() < nDocs || pscore > minScore) {

                            hitq.insert(new ScoreDoc(doc, score, pscore,
                                    updatedates[doc]));
                            minScore = ((ScoreDoc) hitq.top()).pscore;
                        }
                    }
                }
            });

        } else if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_ALL_CLUST) {
            final Map<Integer, ScoreDoc> clustMap = new ConcurrentHashMap<Integer, ScoreDoc>();

            scorer.score(new HitCollector() {
                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    pscore *=scoreParam.getScorePercent();
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {

                        totalHits[0]++;
                        hitq.cluster(tradeids[doc], industryid[doc],scoreParam.getTradeLevel());
                        if (clustMap.get(userids[doc]) != null) {
                            if (pscore > clustMap.get(userids[doc]).pscore) {
                                clustMap.put(userids[doc], new ScoreDoc(doc,
                                        score, pscore));
                            }
                        } else {
                            clustMap.put(userids[doc], new ScoreDoc(doc, score,
                                    pscore));
                        }
                    }
                }
            });
            totalHits[1] = clustMap.size();
            Set<Integer> keySet = clustMap.keySet();
            Iterator<Integer> iter = keySet.iterator();
            float minScore = 0.0f;

            while (iter.hasNext()) {
                int key = iter.next();
                ScoreDoc scoreDoc = clustMap.get(key);
                if (hitq.size() < nDocs || scoreDoc.pscore > minScore) {
                    hitq.insert(scoreDoc);
                    minScore = ((ScoreDoc) hitq.top()).pscore;
                }
            }
        } else if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_ONLY_SEARCH_CLUST) {
            final Map<Integer, ScoreDoc> clustMap = new ConcurrentHashMap<Integer, ScoreDoc>();

            scorer.score(new HitCollector() {

                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    pscore *=scoreParam.getScorePercent();
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {
                        totalHits[0]++;
                        if (clustMap.get(userids[doc]) != null) {
                            if (pscore > clustMap.get(userids[doc]).pscore) {
                                clustMap.put(userids[doc], new ScoreDoc(doc, score, pscore));
                            }
                        } else {
                            clustMap.put(userids[doc], new ScoreDoc(doc, score, pscore));
                        }
                    }
                }
            });
            totalHits[1] = clustMap.size();
            Set<Integer> keySet = clustMap.keySet();
            Iterator<Integer> iter = keySet.iterator();
            float minScore = 0.0f;

            while (iter.hasNext()) {
                int key = iter.next();
                ScoreDoc scoreDoc = clustMap.get(key);
                if (hitq.size() < nDocs || scoreDoc.pscore > minScore) {
                    hitq.insert(scoreDoc);
                    minScore = ((ScoreDoc) hitq.top()).pscore;
                }
            }
        } else if (scoreParam.getSearchType() == ProductConstants.SEARCH_TYPE_TRADE_CLUST) {
            final Map<Integer, ScoreDoc> clustMap = new ConcurrentHashMap<Integer, ScoreDoc>();

            final long[] updatedates = (long[]) MemoryFieldCache.get(reader,
                    "udate");
            scorer.score(new HitCollector() {

                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    float pscore = plugin.score(doc, score);
                    pscore *=scoreParam.getScorePercent();
                    if ((bites == null || bites.get(doc)) && pscore > 0.0f) {
                        totalHits[0]++;
                        hitq.cluster(tradeids[doc], industryid[doc],scoreParam.getTradeLevel());
                        if (clustMap.get(userids[doc]) != null) {
                            if (pscore > clustMap.get(userids[doc]).pscore) {
                                clustMap.put(userids[doc], new ScoreDoc(doc,
                                        score, pscore, updatedates[doc]));
                            }
                        } else {
                            clustMap.put(userids[doc], new ScoreDoc(doc, score,
                                    pscore, updatedates[doc]));
                        }
                        // if (hitq.size() < nDocs || pscore > minScore) {
                        //
                        // hitq.insert(new ScoreDoc(doc, score,
                        // pscore, updatedates[doc]));
                        // minScore = ((ScoreDoc) hitq.top()).pscore;
                        // }
                    }
                }
            });
            totalHits[1] = clustMap.size();
            Set<Integer> keySet = clustMap.keySet();
            Iterator<Integer> iter = keySet.iterator();
            float minScore = 0.0f;

            while (iter.hasNext()) {
                int key = iter.next();
                ScoreDoc scoreDoc = clustMap.get(key);
                if (hitq.size() < nDocs || scoreDoc.pscore > minScore) {
                    hitq.insert(scoreDoc);
                    minScore = ((ScoreDoc) hitq.top()).pscore;
                }
            }
        } else {
            scorer.score(new HitCollector() {
                public final void collect(int doc, float score) {
                    // ignore zeroed buckets
                    if (bites == null || bites.get(doc)) {
                        hitq.cluster(tradeids[doc], industryid[doc],scoreParam.getTradeLevel());
                    }
                }
            });

        }
        return hitq;
    }
}

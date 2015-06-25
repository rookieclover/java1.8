package cn.b2b.index.product.index.score;


import org.apache.lucene.index.IndexReader;

import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.BaseScorePlugin;

public class ProductInTimeScorer extends BaseScorePlugin {
    byte[] memlevels;
    long[] updatetimes;
    // private int[] chapterCount;

    @Override
    public void initCache(IndexReader reader) {
        memlevels = (byte[])MemoryFieldCache.get(reader, "memlevel");
        updatetimes = (long[])MemoryFieldCache.get(reader, "utime");
    }
    /* (non-Javadoc)
     * @see org.apache.lucene.score.ScorePlugin#score(int, float)
     */
    @Override
    public float score(int doc, float score) {
        score += updatetimes[doc] + memlevels[doc] * 10;
        return score;
    }
}

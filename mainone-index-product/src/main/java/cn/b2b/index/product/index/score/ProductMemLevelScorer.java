package cn.b2b.index.product.index.score;

import org.apache.lucene.index.IndexReader;

import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.BaseScorePlugin;

public class ProductMemLevelScorer extends BaseScorePlugin {
    private byte[] memlevels;

    // private int[] chapterCount;

    @Override
    public void initCache(IndexReader reader) {
        memlevels = (byte[]) MemoryFieldCache.get(reader, "memlevel");
    }
    /* (non-Javadoc)
     * @see org.apache.lucene.score.ScorePlugin#score(int, float)
     */
    @Override
    public float score(int doc, float score) {
        score += (memlevels[doc] * 100);
        return score;
    }
}

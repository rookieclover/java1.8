package cn.b2b.index.product.index.score;

import java.util.Calendar;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BitVector;

import cn.b2b.lucene.search.memory.MemoryFieldCache;
import cn.b2b.lucene.search.plugin.BaseScorePlugin;

public class ProductSimilarScorer extends BaseScorePlugin {

  //会员级别：八宝--四宝--基础--免费
  //   1.5 1.3 1.1 1.0
  //时效：最近24小时内更新过加分
  //1.2
  //认证：实名企业发布的产品加分
  //1.1
      byte[] memlevels;
      long[] updatetimes;
      BitVector licenses;
      long time = 0L;
      public void initCache(IndexReader reader) {
          memlevels = (byte[])MemoryFieldCache.get(reader, "memlevel");
          updatetimes = (long[])MemoryFieldCache.get(reader, "udate");
          licenses = (BitVector)MemoryFieldCache.get(reader, "license");
          Calendar date = Calendar.getInstance();
          date.add(Calendar.HOUR, -24);
          time = date.getTime().getTime();
      }
      /* (non-Javadoc)
       * @see org.apache.lucene.score.ScorePlugin#score(int, float)
       */
      @Override
      public float score(int doc, float score) {
          
          switch (memlevels[doc]) {
          case 3:
              score *= 1.5;
              break;
          case 2:
              score *= 1.3;
              break;
          case 1:
              score *= 1.1;
              break;
          }
          if (time < updatetimes[doc]) {
              score *= 1.2;
          }
          if (licenses.get(doc)) {
              score *= 1.1;
          }
          return score;
      }
}

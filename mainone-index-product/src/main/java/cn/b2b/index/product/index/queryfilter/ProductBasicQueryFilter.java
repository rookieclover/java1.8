package cn.b2b.index.product.index.queryfilter;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CvmQuery;
import org.apache.lucene.search.TermQuery;

import cn.b2b.common.search.query.Query;
import cn.b2b.common.search.query.Query.Clause;
import cn.b2b.common.search.query.Query.Term;
import cn.b2b.common.search.query.filter.QueryFilter;
import cn.b2b.common.search.util.Stopwords;
import cn.b2b.index.product.common.ProductConstants;

public class ProductBasicQueryFilter implements QueryFilter {

	// private static float URL_BOOST = 4.0f;
	// private static float ANCHOR_BOOST = 9.0f;
	private static float ANCHOR_BOOST = 200.0f;
	private static float CONTENT_BOOST = 5.0f;
	// private static float KEYWORD_BOOST = 10.0f;

	private static int SLOP = Integer.MAX_VALUE;
	private static float PHRASE_BOOST = 1.0f;

	private static final String[] FIELDS = { "anchor","content"};  
	private static final float[] FIELD_BOOSTS = { ANCHOR_BOOST, CONTENT_BOOST };

	private static int maxTerms = FIELDS.length * 20;

	/**
	 * Set the boost factor for url matches, relative to content and anchor
	 * matches
	 */
	// public static void setUrlBoost(float boost) { URL_BOOST = boost; }

	/**
	 * Set the boost factor for title/anchor matches, relative to url and
	 * content matches.
	 */
	public static void setAnchorBoost(float boost) {
		ANCHOR_BOOST = boost;
	}

	/**
	 * Set the boost factor for sloppy phrase matches relative to unordered term
	 * matches.
	 */
	public static void setPhraseBoost(float boost) {
		PHRASE_BOOST = boost;
	}

	/**
	 * Set the maximum number of terms permitted between matching terms in a
	 * sloppy phrase match.
	 */
	public static void setSlop(int slop) {
		SLOP = slop;
	}

	public BooleanQuery filter(Query input, BooleanQuery output, int slop) {
		addTerms(input, output, slop);
		// addSloppyPhrases(input, output);
		return output;
	}

	private static void addTerms(Query input, BooleanQuery output, int slop) {
		Clause[] clauses = input.getClauses();
		int termNum = 0;
		// For OR query clauses
		ArrayList<BooleanQuery> orQueryList = new ArrayList<BooleanQuery>();
		boolean isProhibited = false;
		for (int i = 0; i < clauses.length; i++) {
			Clause c = clauses[i];
			if (c.isPhrase()) {
				continue;
			}
			if (!c.getField().equals(Clause.DEFAULT_FIELD))
				continue;// skip non-default fields
			BooleanQuery out = new BooleanQuery();
			ArrayList<TermQuery> termQueryList = new ArrayList<TermQuery>();

			String queryStr = c.getTerm().toString();
			if (queryStr == null || queryStr.length() == 0)
				continue;

			BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
			if (c.isProhibited()) {
				occur = BooleanClause.Occur.MUST_NOT;
			} else {
				if (c.isRequired()) {
					occur = BooleanClause.Occur.MUST;
				} else {
					occur = BooleanClause.Occur.SHOULD;
				}
			}

			for (int f = 0; f < FIELDS.length; f++) {
				// System.out.println("sssssqueryStrss----------------------------"+queryStr);
				ArrayList<Term> spanQueryList = new ArrayList<Term>();
				StringTokenizer st = new StringTokenizer(queryStr, "/");
				int wordNum = st.countTokens();
				if (wordNum > 1) {
					while (st.hasMoreTokens()) {
						String wordvalue = st.nextToken();
						if (wordvalue.length() == 0)
							continue;
						String[] values = wordvalue.split("\\|");
						String word = values[0];
						float idf = 1.0f;
						try {
							idf = Float.valueOf(values[1]);
						} catch (Exception e) {
						}

						if (word.length() == 0)
							continue;
						Term term = new Term(word.toLowerCase(), idf);
						if (Stopwords.isHighFreq(word)) {
							termQueryList.add(termQuery(FIELDS[f], term,
									FIELD_BOOSTS[f], true));
							continue;
						}

						if (termNum >= maxTerms) {
							break;
						}
						spanQueryList.add(term);
						termNum++;
						if(spanQueryList.size() >=2 && slop ==  ProductConstants.QUERY_TYPE_MUST_AND_MUST_HAS_NEED_WORD)
						{
							break;
						}
					}
					if (spanQueryList.size() > 1) {
						if (slop == ProductConstants.QUERY_TYPE_MUST_OR) {
							BooleanQuery distanceQuery = new BooleanQuery();
							distanceQuery.setBoost(FIELD_BOOSTS[f]);
							for (int j = 0; j < spanQueryList.size(); j++) {
								Term term = spanQueryList.get(j);
								org.apache.lucene.index.Term l_term = new org.apache.lucene.index.Term(
										FIELDS[f], term.toString(), term.getIdf());
								org.apache.lucene.search.Query q1 = new org.apache.lucene.search.TermQuery(
										l_term);
								distanceQuery.add(q1,
										BooleanClause.Occur.SHOULD);
							}
							out.add(distanceQuery,
									BooleanClause.Occur.SHOULD);
						}
						else {
							BooleanQuery distanceQuery = new BooleanQuery();
							distanceQuery.setBoost(FIELD_BOOSTS[f]);
							for (int j = 0; j < spanQueryList.size(); j++) {
								Term term = spanQueryList.get(j);
								org.apache.lucene.index.Term l_term = new org.apache.lucene.index.Term(
										FIELDS[f], term.toString(), term.getIdf());
								org.apache.lucene.search.Query q1 = new org.apache.lucene.search.TermQuery(
										l_term);
								distanceQuery.add(q1,
										BooleanClause.Occur.MUST);
							}
							out.add(distanceQuery,
									BooleanClause.Occur.SHOULD);
						}

					} else if (spanQueryList.size() > 110) {
						CvmQuery distanceQuery = new CvmQuery(); // Using
																	// cvmquery
						distanceQuery.setSlop(slop);
						distanceQuery.setBoost(FIELD_BOOSTS[f]);
						// boolean span0 = false;
						for (int j = 0; j < spanQueryList.size(); j++) {
							Term term = spanQueryList.get(j);

							org.apache.lucene.index.Term l_term = new org.apache.lucene.index.Term(
									FIELDS[f], term.toString());
							distanceQuery.add(l_term);
						}
						out.add(distanceQuery, BooleanClause.Occur.SHOULD);
					} else if (spanQueryList.size() == 1) {
						out.add(termQuery(FIELDS[f], spanQueryList.get(0),
								FIELD_BOOSTS[f], false),
								BooleanClause.Occur.SHOULD);
					}
				} else {
					if (termNum >= maxTerms)
						break;
					String wordvalue = st.nextToken();
					if (wordvalue.length() == 0)
						continue;
					String[] values = wordvalue.split("\\|");
					String word = values[0];
					float idf = 1.0f;
					try {
						idf = Float.valueOf(values[1]);
					} catch (Exception e) {
					}
					if (word.length() == 0)
						continue;
					Term term = new Term(word.toLowerCase(), idf);
					if (Stopwords.isHighFreq(word)) {
						out.add(termQuery(FIELDS[f], term, FIELD_BOOSTS[f],
								true), BooleanClause.Occur.SHOULD);
					} else
						out.add(termQuery(FIELDS[f], term, FIELD_BOOSTS[f],
								false), BooleanClause.Occur.SHOULD);
					termNum++;
				}
				spanQueryList.clear();
			}
			if (!c.isRequired()) {
				if (orQueryList.size() == 0 && c.isProhibited())
					isProhibited = true;
				orQueryList.add(out);
				continue;
			} else {
				if (orQueryList.size() > 0) {
					BooleanQuery orQuery = new BooleanQuery();
					for (BooleanQuery query : orQueryList)
						orQuery.add(query, BooleanClause.Occur.SHOULD);
					orQueryList.clear();
					if (isProhibited)
						output.add(orQuery, BooleanClause.Occur.MUST_NOT);
					else
						output.add(orQuery, BooleanClause.Occur.MUST);
					isProhibited = false;
				}
				if (out.getClauses().length > 0)
					output.add(out, occur);
			}

			if (termQueryList.size() > 0) {
				BooleanQuery freqTerms = new BooleanQuery();
				for (TermQuery termQuery : termQueryList) {
					if (termNum < maxTerms) {
						freqTerms.add(termQuery, BooleanClause.Occur.SHOULD);
						termNum++;
					} else {
						break;
					}
				}
				output.add(freqTerms, occur);
				termQueryList.clear();
			}
		}
		if (orQueryList.size() > 0) {
			BooleanQuery orQuery = new BooleanQuery();
			for (BooleanQuery query : orQueryList)
				orQuery.add(query, BooleanClause.Occur.SHOULD);
			orQueryList.clear();
			if (isProhibited)
				output.add(orQuery, BooleanClause.Occur.MUST_NOT);
			else
				output.add(orQuery, BooleanClause.Occur.MUST);
			isProhibited = false;
		}
	}

	private static org.apache.lucene.search.TermQuery termQuery(String field,
			Term term, float boost, boolean highFreq) {
		org.apache.lucene.search.TermQuery result = new org.apache.lucene.search.TermQuery(
				luceneTerm(field, term), highFreq);
		result.setBoost(boost);
		return result;
	}

	/** Utility to construct a Lucene Term given a Nutch query term and field. */
	private static org.apache.lucene.index.Term luceneTerm(String field,
			Term term) {
		return new org.apache.lucene.index.Term(field, term.toString());
	}
}

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

public class TestReaderTerm {

	public static void main(String[] args) throws CorruptIndexException,
			IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		IndexReader reader = IndexReader
				.open("E:\\data\\product-index-db\\index");
		TermEnum te = reader.terms();
		while (te.next()) {
				
			Term t = te.term();		
			System.out.println(t.text());
		}

	}
}

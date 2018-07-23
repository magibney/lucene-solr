package org.apache.lucene.search.spans;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;


public class NestedSpanNearTest {
	private static final String FIELD_NM = "text";

	private Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
	private Directory directory;

	@Before
	public void setup() throws Exception {
		directory = new RAMDirectory();
	}
	
	protected static void addDocument(IndexWriter writer, String field, String value) throws IOException {
		Document document = new Document();
		document.add(new TextField(field, value, Store.YES));
		writer.addDocument(document);
	}

	@Test
	public void nestedSpanNearQueryWithRepeatingGroupsShouldFindDocument() throws Exception {
		IndexWriterConfig conf = new IndexWriterConfig(analyzer);
		IndexWriter indexWriter = new IndexWriter(directory, conf);
		String value = "w x a b c d d b c e y z";
		addDocument(indexWriter, FIELD_NM, value);
		indexWriter.close();
		
		Query query = createNestedSpanQuery("a b c d d b c e");
		System.out.println(query);
		
		IndexReader reader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(reader);
		TopDocs topDocs = searcher.search(query, 10);
		assertEquals(1, topDocs.totalHits);
	}

	private SpanQuery createNestedSpanQuery(String queryStr) throws IOException {
		String[] splits = queryStr.split(" ");
		SpanQuery parentClause = null;
		SpanQuery spanQuery = null;
		for (int i = 0 ; i < splits.length; i++) {
			SpanTermQuery spanTermQuery = new SpanTermQuery(new Term(FIELD_NM, splits[i].toLowerCase()));
			SpanQuery[] clauses = parentClause == null ? new SpanQuery[]{spanTermQuery} : new SpanQuery[]{parentClause, spanTermQuery};
      if (clauses.length > 1) {
        spanQuery = new SpanNearQuery(clauses, 2, true);
      } else {
        spanQuery = spanTermQuery;
      }
			parentClause = spanQuery;
		}
		return spanQuery;
	}

}


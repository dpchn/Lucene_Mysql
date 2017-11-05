import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;

public class Searcher {

	public static void main(String[] args) {
		System.out.println("Enter query to Search : ");
		Scanner scanner = new Scanner(System.in);
		String textToFind = scanner.nextLine();
		String indexDir = "D:/Lucene/IndexDir";
		try {
			FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexDir));
			
			IndexReader reader = DirectoryReader.open(fsDirectory);
			IndexSearcher searcher = new IndexSearcher(reader);
			Query query = creatQuery(textToFind);
			TopDocs docs = searcher.search(query, 10);
			if(docs.totalHits==0)
				System.out.println("Not found");
			else{
				for(ScoreDoc scoreDoc : docs.scoreDocs){
				System.out.println(scoreDoc.score);
				System.out.println(searcher.doc(scoreDoc.doc).get("name"));
				System.out.println(searcher.doc(scoreDoc.doc).get("age"));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static Query  creatQuery(String text){
		Analyzer analyzer = new StandardAnalyzer();
		QueryBuilder builder = new QueryBuilder(analyzer);
		Query query = builder.createPhraseQuery("name", text);
		return query;
	}
}


/** 
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

/**
 * The interface to the Lucene index.
 */
public class Idx {

  // --------------- Constants and variables ---------------------

  public static IndexReader INDEXREADER = null;
  private static DocLengthStore DOCLENGTHSTORE;

  // --------------- Methods ---------------------------------------

  /**
   * Get the specified attribute from the specified document.
   * 
   * @param attributeName
   *          Name of attribute
   * @param docid
   *          The internal docid in the lucene index.
   * @return the attribute value
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static String getAttribute(String attributeName, int docid) throws IOException {
    Document d = Idx.INDEXREADER.document(docid);
    return d.get(attributeName);
  }

  /**
   * Get the number of documents that contain the specified field.
   * 
   * @param fieldName
   *          the field name
   * @return the number of documents that contain the field
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static int getDocCount(String fieldName) throws IOException {
    return Idx.INDEXREADER.getDocCount(fieldName);
  }

  /**
   * Get the external document id for a document specified by an internal
   * document id.
   * 
   * @param iid
   *          The internal document id of the document.
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static String getExternalDocid(int iid) throws IOException {
    Document d = Idx.INDEXREADER.document(iid);
    String eid = d.get("externalId");
    return eid;
  }

  /**
   * Get the length of the specified field in the specified document.
   * 
   * @param fieldName
   *          Name of field to access lengths.
   * @param docid
   *          The internal docid in the lucene index.
   * @return the length of the field, including stopword positions.
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static int getFieldLength(String fieldName, int docid) throws IOException {
    return (int) Idx.DOCLENGTHSTORE.getDocLength(fieldName, docid);
  }

  /**
   * Get the internal document id for a document specified by its external id,
   * e.g. clueweb09-enwp00-88-09710. If no such document exists, throw an
   * exception.
   * 
   * @param externalId
   * @return iternal docid.
   * @throws Exception
   *           Could not read the internal document id from the index.
   */
  public static int getInternalDocid(String externalId) throws Exception {

    Query q = new TermQuery(new Term("externalId", externalId));

    IndexSearcher searcher = new IndexSearcher(Idx.INDEXREADER);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1, false);
    searcher.search(q, collector);
    ScoreDoc[] hits = collector.topDocs().scoreDocs;

    if (hits.length < 1) {
      return -1;
    } else {
      return hits[0].doc;
    }
  }

  /**
   * Get the total number of documents in the corpus.
   * 
   * @return The total number of documents.
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static long getNumDocs() throws IOException {
    return Idx.INDEXREADER.numDocs();
  }

  /**
   * Get the total number of term occurrences contained in all instances of the
   * specified field in the corpus (e.g., add up the lengths of every TITLE
   * field in the corpus).
   * 
   * @param fieldName
   *          The field name.
   * @return The total number of term occurrence
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  public static long getSumOfFieldLengths(String fieldName) throws IOException {
    return Idx.INDEXREADER.getSumTotalTermFreq(fieldName);
  }

  /**
   * Open a Lucene index and the associated DocLengthStore.
   * 
   * @param indexPath
   *          A directory that contains a Lucene index.
   * @throws IllegalArgumentException
   *           Unable to open the index.
   * @throws IOException
   *           Error accessing the index.
   */
  public static void initialize(String indexPath) throws IllegalArgumentException, IOException {

    // Open the Lucene index

    Idx.INDEXREADER = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

    if (Idx.INDEXREADER == null) {
      throw new IllegalArgumentException("Unable to open the index.");
    }

    // Lucene doesn't store field lengths the way that we want them,
    // so we have our own document length store.

    Idx.DOCLENGTHSTORE = new DocLengthStore(Idx.INDEXREADER);

    if (Idx.DOCLENGTHSTORE == null) {
      throw new IllegalArgumentException("Unable to open the document length store.");
    }
  }

}

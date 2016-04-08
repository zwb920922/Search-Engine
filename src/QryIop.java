
/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 * All query operators that return inverted lists are subclasses of the QryIop
 * class. This class has two main purposes. First, it allows query operators to
 * easily recognize any nested query operator that returns an inverted list
 * (e.g., #AND (a #NEAR/1 (b c)). Second, it is a place to store data structures
 * and methods that are common to all query operators that return inverted
 * lists.
 * </p>
 * <p>
 * After a QryIop operator is initialized, it caches a full inverted list, and
 * information from the inverted list is accessible. Document and location
 * information are accessed via Qry.docIterator and QryIop.locIterator.
 * Corpus-level information, for example, document frequency (df) and collection
 * term frequency (ctf), are available via specific methods (e.g., getDf and
 * getCtf).
 * </p>
 * <p>
 * QryIop operators support iteration over the locations in the document that
 * Qry.docIteratorHasMatch matches. The semantics and use of the
 * QryIop.locIterator are similar to the Qry.docIterator. The QryIop.locIterator
 * is initialized automatically each time Qry.docIteratorHasMatch finds a match;
 * no additional initialization is required.
 */
public abstract class QryIop extends Qry {

  /*
   * IMPLEMENTATION NOTES:
   * 
   * Iteration in QryIop and QrySop is very different. In QryIop, docIterator
   * and locIterator iterate over the cached inverted list, NOT recursively over
   * the query arguments.
   */

  /**
   * An invalid index for docIterator and locIterator.
   */
  private static final int INVALID_ITERATOR_INDEX = -1;

  /**
   * The document field that the query operator applies to; this is inferred
   * from query operator arguments.
   */
  protected String field = null;

  /**
   * The inverted list that is produced when the query operator is initialized;
   * use the docIterator to access this list.
   */
  protected InvList invertedList = null;

  /**
   * The index of the document that the docIterator points to now.
   */
  private int docIteratorIndex = QryIop.INVALID_ITERATOR_INDEX;

  /**
   * The index of the location that the locIterator points to now.
   */
  private int locIteratorIndex = QryIop.INVALID_ITERATOR_INDEX;
  
  /**
   * Advance the query operator's internal iterator beyond the specified
   * document.
   * 
   * @param docid
   *          The document's internal document id
   */
  public void docIteratorAdvancePast(int docid) {

    while ((this.docIteratorIndex < this.invertedList.df)
        && (this.invertedList.getDocid(this.docIteratorIndex) <= docid)) {
      this.docIteratorIndex++;
    }

    this.locIteratorIndex = 0;
  }

  /**
   * Advance the query operator's internal iterator to the specified document if
   * it exists, or beyond if it doesn't.
   * 
   * @param docid
   *          The document's internal document id
   */
  public void docIteratorAdvanceTo(int docid) {

    while ((this.docIteratorIndex < this.invertedList.df)
        && (this.invertedList.getDocid(this.docIteratorIndex) < docid)) {
      this.docIteratorIndex++;
    }

    this.locIteratorIndex = 0;
  }

  /**
   * Advance the query operator's internal iterator beyond the any possible
   * document.
   */
  public void docIteratorFinish() {
    this.docIteratorIndex = this.invertedList.postings.size();
  }

  /**
   * Return the id of the document that the query operator's internal iterator
   * points to now. Use dociIteratorHasMatch to determine whether the iterator
   * currently points to a document. If the iterator doesn't point to a
   * document, an invalid document id is returned.
   * 
   * @return The internal id of the current document.
   */
  @Override
  public int docIteratorGetMatch() {
    return this.invertedList.getDocid(this.docIteratorIndex);
  }

  /**
   * Return the postings for the document that the docIterator points to now, or
   * throw an error if the docIterator doesn't point at a document.
   * 
   * @return A document posting.
   */
  public InvList.DocPosting docIteratorGetMatchPosting() {
    return this.invertedList.postings.get(docIteratorIndex);
  }

  /**
   * Indicates whether the query has a matching document.
   * 
   * @param r
   *          A retrieval model (that is ignored - it can be null)
   * @return True if the query matches a document, otherwise false.
   */
  public boolean docIteratorHasMatch(RetrievalModel r) {
    return (this.docIteratorIndex < this.invertedList.df);
  }
  
  /**
   * Return the TF of the document that the iterator points to now. 
   * 
   * @return The internal id of the current document.
   */
  @Override
  public int docIteratorGetMatchTF() {
    return invertedList.getTf(docIteratorIndex);
  }
  
  /**
   * Get the collection term frequency (ctf) associated with this query
   * operator. It is an error to call this method before the object's initialize
   * method is called.
   * 
   * @return The collection term frequency (ctf).
   */
  public int getCtf() {
    return this.invertedList.ctf;
  }

  /**
   * Get the document frequency (df) associated with this query operator. It is
   * an error to call this method before the object's initialize method is
   * called.
   * 
   * @return The document frequency (df).
   */
  public int getDf() {
    return this.invertedList.df;
  }

  /**
   * Get the field associated with this query operator.
   * 
   * @return The field associated with this query operator.
   */
  public String getField() {
    return this.field;
  }

  /**
   * Evaluate the query operator; the result is an internal inverted list that
   * may be accessed via the internal iterators.
   * 
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  protected abstract void evaluate() throws IOException;

  /**
   * Initialize the query operator (and its arguments), including any internal
   * iterators; this method must be called before iteration can begin.
   * 
   * @param r
   *          A retrieval model (that is ignored)
   */
  public void initialize(RetrievalModel r) throws IOException {

    // Initialize the query arguments (if any).

    for (Qry q_i : this.args) {
      ((QryIop) q_i).initialize(r);
    }

    // Evaluate the operator.

    this.evaluate();

    // Initialize the internal iterators.

    this.docIteratorIndex = 0;
    this.locIteratorIndex = 0;
  }

  /**
   * Advance the query operator's internal iterator to the next location.
   */
  public void locIteratorAdvance() {
    this.locIteratorIndex++;
  }

  /**
   * Advance the query operator's internal iterator beyond the specified
   * location.
   * 
   * @param loc
   *          The location to advance beyond.
   */
  public void locIteratorAdvancePast(int loc) {
    int tf = this.invertedList.postings.get(this.docIteratorIndex).tf;
    Vector<Integer> positions = this.invertedList.postings.get(this.docIteratorIndex).positions;

    while ((this.locIteratorIndex < tf) && (positions.get(this.locIteratorIndex) <= loc)) {
      locIteratorIndex++;
    }
  }

  /**
   * Advance the query operator's internal iterator beyond the any possible
   * location.
   */
  public void locIteratorFinish() {
    this.locIteratorIndex = this.invertedList.postings.get(this.docIteratorIndex).tf;
  }

  /**
   * Return the document location that the query operator's internal iterator
   * points to now. Use iterHasLoc to determine whether the iterator currently
   * points to a location. If the iterator doesn't point to a location, an
   * invalid document location is returned.
   * 
   * @return The internal id of the current document.
   */
  public int locIteratorGetMatch() {
    Vector<Integer> locations = this.docIteratorGetMatchPosting().positions;
    return locations.get(this.locIteratorIndex);
  }

  /**
   * Returns true if the query operator's internal iterator currently points to
   * a location.
   * 
   * @return True if the iterator currently points to a location.
   */
  public boolean locIteratorHasMatch() {
    return (this.locIteratorIndex < this.invertedList.getTf(this.docIteratorIndex));
  }
 
  /////////////////// helpers for getScoreForBM25Sum////////////////////////
  /**
   * get RSJ weight(idf) for Okapi BMxx Model.
   * @return RSJ weight
   * @throws IOException
   */
  public double getIdf() throws IOException {
    double numerator = Idx.getNumDocs() - this.invertedList.df + 0.5;
    double denominator = this.invertedList.df + 0.5;
    return Math.max(0, Math.log(numerator / denominator));
  }
  
  /**
   * get tf weight for Okapi BMxx Model.
   * @param k_1 must >= 1.0
   * @param b must between 0.0 and 1.0
   * @return tf weight
   * @throws IOException
   */
  public double getTfWeight(double k_1, double b) throws IOException {
    double tf = docIteratorGetMatchTF();
    double docLen = Idx.getFieldLength(field, this.invertedList.getDocid(docIteratorIndex)) ;
    double avg_docLen = ((double) Idx.getSumOfFieldLengths(field)) / Idx.getDocCount(field);
    double temp = tf + k_1 * ((1 - b) + b * docLen / avg_docLen);
    return tf / temp;
  }
  
  /**
   * get user weight for Okapi BMxx Model.
   * @param k_3 must >= 0
   * @return user weight
   */
  public double getUserWeight(double k_3) {
    double qtf = 1; // not sure ?
    return (k_3 + 1) * qtf / (k_3 + qtf);    
  }
  /**
   * get score for the doci of the queryi in BM25.
   * @param k_1
   * @param b
   * @param k_3
   * @return
   * @throws IOException
   */
  public double getScoreForBM25Sum(RetrievalModelBM25 r) throws IOException {
    double k_1 = r.getK_1();
    double b = r.getB();
    double k_3 = r.getK_3();
    return getIdf() * getTfWeight(k_1, b) * getUserWeight(k_3);
  }
  
  //////////////////////// helpers for getScoreForIndriAnd /////////////////////
  /**
   * get score for the doci of the queryi in Indri.
   * @param mu
   * @param lambda
   * @return
   * @throws IOException
   */
  public double getScoreForIndri(RetrievalModelIndri r) throws IOException {
    double tf = docIteratorGetMatchTF();
    double docLen = Idx.getFieldLength(field, this.invertedList.getDocid(docIteratorIndex)) ;
    double corpLen = Idx.getSumOfFieldLengths(field);
    double ctf = this.invertedList.ctf;
    double mu = r.getMu();
    double lambda = r.getLambda();
    return (1 - lambda) * ((tf + mu * ctf / corpLen) / (docLen + mu)) + lambda * ctf / corpLen;   
  }
  /**
   * get default score for the doci of the queryi in Indri
   * when the doc is not in the inverted list.
   * @param mu
   * @param lambda
   * @return
   * @throws IOException
   */
  public double getDefaultScoreForIndri(RetrievalModelIndri r, int docid) throws IOException {
    double docLen = Idx.getFieldLength(field, docid) ;
    double corpLen = Idx.getSumOfFieldLengths(field);
    double ctf = this.invertedList.ctf;
    double mu = r.getMu();
    double lambda = r.getLambda();
    return (1 - lambda) * ( mu * ctf / corpLen / (docLen + mu)) + lambda * ctf / corpLen; 
  }
  
}

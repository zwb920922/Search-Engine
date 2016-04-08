import java.io.IOException;

/**
 * The AND operator for all retrieval models.
 * 
 * @author zhangwenbo
 *
 */
public class QrySopAnd extends QrySop {

  /**
   * constructor of Boolean AND operator.
   */
  public QrySopAnd() {
    // do nothing here.
  }

  /**
   * Get a score for the document that docIteratorHasMatch matched.
   * 
   * @param r
   *          The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException
   *           Error accessing the Lucene index
   */
  public double getScore(RetrievalModel r) throws IOException {

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean(r);
    } else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean(r);
    } else if (r instanceof RetrievalModelIndri) {
      return this.getScoreIndri((RetrievalModelIndri) r);
    } else {
      throw new IllegalArgumentException(r.getClass().getName() + " doesn't support the And operator.");
    }
  }
  
  public double getDefaultScore(RetrievalModel r, int docid) throws IOException {
    return getDefaultScoreIndri(((RetrievalModelIndri) r), docid);
  }

  /**
   * getScore for the UnrankedBoolean retrieval model.
   * 
   * @param r
   *          The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException
   *           Error accessing the Lucene index
   */
  private double getScoreUnrankedBoolean(RetrievalModel r) throws IOException {
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  /**
   * getScore for the RankedBoolean retrieval model.
   * 
   * @param r
   *          The retrieval model that determines how scores are calculated.
   * @return The document score.
   * @throws IOException
   *           Error accessing the Lucene index
   */
  private double getScoreRankedBoolean(RetrievalModel r) throws IOException {
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      double score = -1;
      for (Qry q : args) {
        double temp = ((QrySop) q).getScore(r);
        if (score == -1 || temp < score)
          score = temp;
      }
      return score;
    }
  }

  /**
   * getScore for the Indri retrieval model.
   * 
   * @param r
   *          retrieval model
   * @return score
   * @throws IOException
   */
  private double getScoreIndri(RetrievalModelIndri r) throws IOException {
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      if (args.size() == 0)
        return 0.0;
      double ret = 1;
      double power = 1.0 / this.args.size();
      int docid = this.docIteratorGetMatch();
      for (Qry q_i : this.args) {
        //System.out.println(q_i + ": " + q_i.docIteratorGetMatch());
        if (q_i.docIteratorHasMatch(r) && q_i.docIteratorGetMatch() == docid)
          ret *= Math.pow(((QrySop) q_i).getScore(r), power);
        else
          ret *= Math.pow(((QrySop) q_i).getDefaultScore(r, docid), power);
      }
      return ret;
    }
  }
  /**
   * get default score for the Indri retrieval model there is no match in this query.
   * @param r
   * @param docid
   * @return
   * @throws IOException
   */
  private double getDefaultScoreIndri(RetrievalModelIndri r, int docid) throws IOException {
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      if (args.size() == 0)
        return 0.0;
      double ret = 1;
      double power = 1.0 / this.args.size();
      for (Qry q_i : this.args) {
         ret *= Math.pow(((QrySop) q_i).getDefaultScore(r, docid), power);
      }
      return ret;
    }
  }
  
  /**
   * Indicates whether the query has a match.
   * 
   * @param r
   *          The retrieval model that determines what is a match
   * @return True if the query matches, otherwise false.
   */
  @Override
  public boolean docIteratorHasMatch(RetrievalModel r) {
    if (r instanceof RetrievalModelIndri)
      return this.docIteratorHasMatchMin(r);
    return this.docIteratorHasMatchAll(r);
  }

}

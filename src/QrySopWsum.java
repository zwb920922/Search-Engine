import java.io.IOException;
import java.util.ArrayList;

public class QrySopWsum extends QrySop implements WeightedQry {
  
  /**
   * weights for the query arguments in it.
   */
  protected ArrayList<Double> weights = new ArrayList<>();
  /**
   * add weight of an argument in this weighted query.
   */
  public void appendWeight(double d) {
    weights.add(d);
  }
  
  @Override
  public double getScore(RetrievalModel r) throws IOException {
    return this.getScoreIndri((RetrievalModelIndri) r);
  }

  @Override
  public double getDefaultScore(RetrievalModel r, int docid) throws IOException {
    return this.getDefaultScoreIndri((RetrievalModelIndri) r, docid);
  }

  @Override
  public boolean docIteratorHasMatch(RetrievalModel r) {
    return this.docIteratorHasMatchMin(r);
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
    if (args.size() != weights.size())
      throw new IllegalStateException();
    int size = args.size();
    double weightSum = 0.0;
    for (double weight: weights) 
      weightSum += weight;
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      if (args.size() == 0)
        return 0.0;
      double ret = 0;
      int docid = this.docIteratorGetMatch();
      for (int i = 0; i < size; i++) {
        Qry q_i = args.get(i);
        double weight = weights.get(i);
        //System.out.println(q_i + ": " + q_i.docIteratorGetMatch());
        if (q_i.docIteratorHasMatch(r) && q_i.docIteratorGetMatch() == docid)
          ret += weight/weightSum * ((QrySop) q_i).getScore(r);
        else
          ret += weight/weightSum * ((QrySop) q_i).getDefaultScore(r, docid);
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
    if (args.size() != weights.size())
      throw new IllegalStateException();
    int size = args.size();
    double weightSum = 0.0;
    for (double weight: weights) 
      weightSum += weight;
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      if (args.size() == 0)
        return 0.0;
      double ret = 0;
      for (int i = 0; i < size; i++) {
        Qry q_i = args.get(i);
        double weight = weights.get(i);
        ret += weight/weightSum * ((QrySop) q_i).getDefaultScore(r, docid);
      }
      return ret;
    }
  }
}


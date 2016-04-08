import java.io.IOException;

/**
 * The AND operator for all retrieval models.
 * 
 * @author zhangwenbo
 *
 */
public class QrySopSum extends QrySop {

  /**
   * parameters of Opaki BMxx Model.
   */
  private double k_1, b, k_3;

  @Override
  public double getScore(RetrievalModel r) throws IOException {
    if (r instanceof RetrievalModelBM25) {
      return getScoreBM25(r);
    } else {
      throw new IllegalArgumentException(r.getClass().getName() + " doesn't support the OR operator.");
    }
  }

  private double getScoreBM25(RetrievalModel r) throws IOException {
    if (!this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      if (args.size() == 0) {
        return 0.0;
      } else {
        double ret = 0;
        for (Qry q_i : this.args) {
          if (q_i.docIteratorHasMatch(r) && q_i.docIteratorGetMatch() == this.docIteratorGetMatch()) {
            ret += ((QrySop) q_i).getScore(r);
          }
        }
        return ret;
      }
    }
  }

  @Override
  public boolean docIteratorHasMatch(RetrievalModel r) {
    return this.docIteratorHasMatchMin(r);
  }

  @Override
  public double getDefaultScore(RetrievalModel r, int docid) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}

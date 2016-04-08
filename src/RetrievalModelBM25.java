

/**
 *  An object that stores parameters for the ranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelBM25 extends RetrievalModel {

  private double k_1, b, k_3;
  
  public RetrievalModelBM25(double k_1, double b, double k_3) {
    this.k_1 = k_1;
    this.b = b;
    this.k_3 = k_3;
  }
  
  @Override
  public String defaultQrySopName() {
    return new String("#sum");
  }
  
  public double getK_1() {
    return k_1;
  }

  public double getB() {
    return b;
  }
  
  public double getK_3() {
    return k_3;
  }
}

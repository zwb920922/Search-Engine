

/**
 *  An object that stores parameters for the ranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelIndri extends RetrievalModel {
  
  private double mu, lambda;
  
  public RetrievalModelIndri(double mu, double lambda) {
    this.mu = mu;
    this.lambda = lambda;
  }

  @Override
  public String defaultQrySopName() {
    return new String("#and");
  }
  
  public double getMu() {
    return mu;
  }
  
  public double getLambda() {
    return lambda;
  }

}

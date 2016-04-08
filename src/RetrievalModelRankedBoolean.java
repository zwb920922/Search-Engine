

/**
 *  An object that stores parameters for the ranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelRankedBoolean extends RetrievalModel {

  @Override
  public String defaultQrySopName() {
    return new String("#and");
  }

}

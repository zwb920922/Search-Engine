import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureVectors {

  private String qid;
  
  // doc -> all feature scores for this (q, d) pair
  // double[0] represents the relevant degree of the doc
  private Map<String, Double[]> featureScoresMap;
  
  public FeatureVectors(String id) {
    qid = id;
    featureScoresMap = new LinkedHashMap<>();
  }
  
  public void addDoc(String docid) {
    if (featureScoresMap.get(docid) != null)
      throw new IllegalArgumentException();
    featureScoresMap.put(docid, new Double[19]);
  }
  
  public void setFeatureValue(String docid, int index, Double value) {
    Double[] features;
    if ((features = featureScoresMap.get(docid)) == null)
      throw new IllegalArgumentException();
    features[index] = value;
  }
  
  public Double getValue(String docid, int i) {
    Double[] a = featureScoresMap.get(docid);
    return Arrays.copyOf(a, a.length)[i];
  }
//  
//  public void setValue(String docid, int i, double value) {
//    double[] a = featureScoresMap.get(docid);
//    a[i] = value;
//  }
  
  public Iterable<String> docids() {
    return featureScoresMap.keySet();
  }
  
  public String getQid() {
    return qid;
  }
}

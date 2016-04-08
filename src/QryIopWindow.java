import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The WINDOW operator for BM25 retrieval models.
 */
public class QryIopWindow extends QryIop {

  /**
   * max distance between two locations.
   */
  private int dist;

  /**
   * constructor.
   * 
   * @param dist
   *          distance
   */
  public QryIopWindow(int dist) {
    this.dist = dist;
  }

  @Override
  protected void evaluate() throws IOException {
    
    // Create an empty inverted list. If there are no query arguments,
    // that's the final result.

    this.invertedList = new InvList(this.getField());

    if (args.size() == 0) {
      return;
    }
    
    boolean matchFound = false;

    // Keep trying until a match is found or no match is possible.
    while (true) {
   // Get the docid of the first query argument.

      Qry q_0 = this.args.get(0);

      if (!q_0.docIteratorHasMatch(null)) {
        return;
      }

      int docid_0 = q_0.docIteratorGetMatch();

      // Other query arguments must match the docid of the first query
      // argument.

      matchFound = true;
      
      for (int i = 1; i < this.args.size(); i++) {
        Qry q_i = this.args.get(i);

        q_i.docIteratorAdvanceTo(docid_0);

        if (!q_i.docIteratorHasMatch(null)) { // If any argument is exhausted
          return; // there are no more matches.
        }

        int docid_i = q_i.docIteratorGetMatch();

        if (docid_0 != docid_i) { // docid_0 can't match. Try again.
          q_0.docIteratorAdvanceTo(docid_i);
          matchFound = false;
          break;
        }
      }
      // find all the positions that match the query in this doc.
      if (matchFound) {
        // similar to the doc search above.
        List<Integer> positions = findPos();
        if (!positions.isEmpty())
          this.invertedList.appendPosting(docid_0, positions);
        ((QryIop) args.get(0)).docIteratorAdvancePast(docid_0);
      }
    }
  }
  /**
   * find all the mathches in the doc, require all the inverted lists pointing to the same doc.
   * @return positions list
   */
  private List<Integer> findPos() {
    List<Integer> positions = new ArrayList<Integer>();
    boolean end = false; // check whether there is still positions in the doc for each word
    int size = args.size();
    int maxLoc = Integer.MIN_VALUE;
    int minLoc = Integer.MAX_VALUE;
    while (!end) {
      for (int i = 0; i < size; i++) {
        QryIop q = (QryIop) args.get(i);
        if (!q.locIteratorHasMatch()) {
          end = true;
          break;
        }
        int cur = q.locIteratorGetMatch();
        if (cur < minLoc)
          minLoc = cur;
        if (cur > maxLoc)
          maxLoc = cur;
      }
      if (!end) {
        if (maxLoc - minLoc < dist) {
          positions.add(minLoc);
          for (int i = 0; i < args.size(); i++) {
            QryIop q = (QryIop) args.get(i);
            q.locIteratorAdvance();
          }
        } else {
          int min = maxLoc - dist;
          for (int i = 0; i < args.size(); i++) {
            QryIop q = (QryIop) args.get(i);
            q.locIteratorAdvancePast(min);
          }
        }
        maxLoc = Integer.MIN_VALUE;
        minLoc = Integer.MAX_VALUE;
      }
    }
    return positions;
  }
  
}

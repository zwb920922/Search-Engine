import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The NEAR operator for all retrieval models.
 * 
 * This operator is similar to the docIteratorHasMatchAll in Qry class. The only difference
 * is that this operator will find all the match rather than the first match.
 * 
 * @author zhangwenbo
 *
 */
public class QryIopNear extends QryIop {
  /**
   * max distance between two locations.
   */
  private int dist;
  /**
   * constructor.
   * 
   * @param dist distance
   */
  public QryIopNear(int dist) {
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
   * find all the positions that mathches and store them in the list.
   * @param positions target list
   */
  private List<Integer> findPos() {
    List<Integer> positions = new ArrayList<Integer>();
    boolean locFound;
    boolean end = false;
    while (!end) {
      locFound = true;
      // find the position of the first qry.
      QryIop qi_0 = ((QryIop) args.get(0));
      
      if (!qi_0.locIteratorHasMatch())
        break;
      int pre = qi_0.locIteratorGetMatch();
      int cur = qi_0.locIteratorGetMatch();
      int size = args.size();
      for (int i = 1; i < size; i++) {
        QryIop q = (QryIop) args.get(i);
        q.locIteratorAdvancePast(pre);
        if (!q.locIteratorHasMatch()) {
          locFound = false;
          end = true;
          break;
        }
        cur = q.locIteratorGetMatch();
        if (cur - pre > this.dist) {
          locFound = false;
          break;
        }
        pre = cur;
      }
      
      if (locFound) {
        positions.add(pre);
        for (int i = 1; i < args.size(); i++) {
          QryIop q = (QryIop) args.get(i);
          q.locIteratorAdvance();
        }
      }
      // advance the doc of the first qry.
      qi_0.locIteratorAdvance();
    }
    return positions;
  }
}

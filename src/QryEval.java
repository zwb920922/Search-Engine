
/*
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.1.1.
 */

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * QryEval is a simple application that reads queries from a file, evaluates
 * them against an index, and writes the results to an output file. This class
 * contains the main method, a method for reading parameter and query files,
 * initialization methods, a simple query parser, a simple query processor, and
 * methods for reporting results.
 * <p>
 * This software illustrates the architecture for the portion of a search engine
 * that evaluates queries. It is a guide for class homework assignments, so it
 * emphasizes simplicity over efficiency. Everything could be done more
 * efficiently and elegantly.
 * <p>
 * The {@link Qry} hierarchy implements query evaluation using a 'document at a
 * time' (DaaT) methodology. Initially it contains an #OR operator for the
 * unranked Boolean retrieval model and a #SYN (synonym) operator for any
 * retrieval model. It is easily extended to support additional query operators
 * and retrieval models. See the {@link Qry} class for details.
 * <p>
 * The {@link RetrievalModel} hierarchy stores parameters and information
 * required by different retrieval models. Retrieval models that need these
 * parameters (e.g., BM25 and Indri) use them very frequently, so the
 * RetrievalModel class emphasizes fast access.
 * <p>
 * The {@link Idx} hierarchy provides access to information in the Lucene index.
 * It is intended to be simpler than accessing the Lucene index directly.
 * <p>
 * As the search engine becomes more complex, it becomes useful to have a
 * standard approach to representing documents and scores. The {@link ScoreList}
 * class provides this capability.
 */
public class QryEval {

  // --------------- Constants and variables ---------------------

  private static final String USAGE = "Usage:  java QryEval paramFile\n\n";

  private static final EnglishAnalyzerConfigurable ANALYZER = new EnglishAnalyzerConfigurable(Version.LUCENE_43);
  private static final String[] TEXT_FIELDS = { "body", "title", "url", "inlink" };

  // --------------- Methods ---------------------------------------

  /**
   * @param args
   *          The only argument is the parameter file name.
   * @throws Exception
   *           Error accessing the Lucene index.
   */
  public static void main(String[] args) throws Exception {

    // This is a timer that you may find useful. It is used here to
    // time how long the entire program takes, but you can move it
    // around to time specific parts of your code.

    Timer timer = new Timer();
    timer.start();

    // Check that a parameter file is included, and that the required
    // parameters are present. Just store the parameters. They get
    // processed later during initialization of different system
    // components.

    if (args.length < 1) {
      throw new IllegalArgumentException(USAGE);
    }

    Map<String, String> parameters = readParameterFile(args[0]);

    // Configure query lexical processing to match index lexical
    // processing. Initialize the index and retrieval model.

    ANALYZER.setLowercase(true);
    ANALYZER.setStopwordRemoval(true);
    ANALYZER.setStemmer(EnglishAnalyzerConfigurable.StemmerType.KSTEM);

    Idx.initialize(parameters.get("indexPath"));
    RetrievalModel model = initializeRetrievalModel(parameters);

    // Perform experiments.

    processQueryFile(parameters, model);

    // Clean up.

    timer.stop();
    System.out.println("Time:  " + timer);
  }

  /**
   * Allocate the retrieval model and initialize it using parameters from the
   * parameter file.
   * 
   * @return The initialized retrieval model
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  private static RetrievalModel initializeRetrievalModel(Map<String, String> parameters) throws IOException {

    RetrievalModel model = null;
    String modelString = parameters.get("retrievalAlgorithm").toLowerCase();

    if (modelString.equals("unrankedboolean")) {
      model = new RetrievalModelUnrankedBoolean();
    } else if (modelString.equals("rankedboolean")) {
      model = new RetrievalModelRankedBoolean();
    } else if (modelString.equals("bm25")) {
      double k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
      double b = Double.parseDouble(parameters.get("BM25:b"));
      double k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
      model = new RetrievalModelBM25(k_1, b, k_3);
    } else if (modelString.equals("indri")) {
      double mu = Double.parseDouble(parameters.get("Indri:mu"));
      double lambda = Double.parseDouble(parameters.get("Indri:lambda"));
      model = new RetrievalModelIndri(mu, lambda);
    } else {
      throw new IllegalArgumentException("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }

    return model;
  }

  /**
   * Optimize the query by removing degenerate nodes produced during query
   * parsing, for example '#NEAR/1 (of the)' which turns into '#NEAR/1 ()' after
   * stopwords are removed; and unnecessary nodes or subtrees, such as #AND
   * (#AND (a)), which can be replaced by 'a'.
   */
  static Qry optimizeQuery(Qry q) {

    // Term operators don't benefit from optimization.

    if (q instanceof QryIopTerm) {
      return q;
    }

    // Optimization is a depth-first task, so recurse on query
    // arguments. This is done in reverse to simplify deleting
    // query arguments that become null.

    for (int i = q.args.size() - 1; i >= 0; i--) {

      Qry q_i_before = q.args.get(i);
      Qry q_i_after = optimizeQuery(q_i_before);

      if (q_i_after == null) {
        q.removeArg(i); // optimization deleted the argument
      } else {
        if (q_i_before != q_i_after) {
          q.args.set(i, q_i_after); // optimization changed the
                                    // argument
        }
      }
    }

    // If the operator now has no arguments, it is deleted.

    if (q.args.size() == 0) {
      return null;
    }

    // Only SCORE operators can have a single argument. Other
    // query operators that have just one argument are deleted.

    if ((q.args.size() == 1) && (!(q instanceof QrySopScore))) {
      q = q.args.get(0);
    }

    return q;

  }

  /**
   * Return a query tree that corresponds to the query.
   * 
   * @param qString
   *          A string containing a query.
   * @param qTree
   *          A query tree
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  static Qry parseQuery(String qString, RetrievalModel model, Map<String, String> parameters) throws IOException {

    // Add a default query operator to every query. This is a tiny
    // bit of inefficiency, but it allows other code to assume
    // that the query will return document ids and scores.

    String defaultOp = model.defaultQrySopName();
    qString = defaultOp + "(" + qString + ")";

    // System.out.println(qString);
    // Simple query tokenization. Terms like "near-death" are handled later.

    StringTokenizer tokens = new StringTokenizer(qString, "\t\n\r ,()", true);
    String token = null;

    // This is a simple, stack-based parser. These variables record
    // the parser's state.

    Qry currentOp = null;
    Stack<Qry> opStack = new Stack<Qry>();
    // check whether the next token can be a weight.
    boolean weightExpected = false;
    Stack<Double> weightStack = new Stack<Double>();

    // Each pass of the loop processes one token. The query operator
    // on the top of the opStack is also stored in currentOp to
    // make the code more readable.

    while (tokens.hasMoreTokens()) {

      // System.out.println(currentOp);
      token = tokens.nextToken();
      // System.out.println("this token is " + token);
      if (token.matches("[ ,(\t\n\r]")) {
        continue;
      } else if (token.equals(")")) { // Finish current query op.

        // If the current query operator is not an argument to another
        // query operator (i.e., the opStack is empty when the current
        // query operator is removed), we're done (assuming correct
        // syntax - see below).
        // System.out.println("this qry is gonna be popped: " +
        // opStack.peek());
        opStack.pop();

        if (opStack.empty())
          break;

        // Not done yet. Add the current operator as an argument to
        // the higher-level operator, and shift processing back to the
        // higher-level operator.

        Qry arg = currentOp;
        currentOp = opStack.peek();
        currentOp.appendArg(arg, model);
        if (currentOp instanceof WeightedQry) {
          ((WeightedQry) currentOp).appendWeight(weightStack.pop());
          weightExpected = true;
        }
      } else if (token.equalsIgnoreCase("#or")) {
        currentOp = new QrySopOr();
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.equalsIgnoreCase("#and")) {
        if (model instanceof RetrievalModelIndri) {
          currentOp = new QrySopAnd();
        } else {
          currentOp = new QrySopAnd();
        }
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.equalsIgnoreCase("#syn")) {
        currentOp = new QryIopSyn();
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.length() > 5 && token.substring(0, 5).equalsIgnoreCase("#near")) {
        currentOp = new QryIopNear(Integer.parseInt(token.substring(6)));
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.length() > 7 && token.substring(0, 7).equalsIgnoreCase("#window")) {
        currentOp = new QryIopWindow(Integer.parseInt(token.substring(8)));
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.equalsIgnoreCase("#sum")) {
        currentOp = new QrySopSum();
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
      } else if (token.equalsIgnoreCase("#wand")) {
        currentOp = new QrySopWand();
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
        weightExpected = true;
      } else if (token.equalsIgnoreCase("#wsum")) {
        currentOp = new QrySopWsum();
        currentOp.setDisplayName(token);
        opStack.push(currentOp);
        weightExpected = true;
      } else if (weightExpected) {
        weightStack.push(Double.parseDouble(token));
        weightExpected = false;
      } else {
        // Split the token into a term and a field.

        int delimiter = token.lastIndexOf('.');
        String field = null;
        String term = null;

        if (delimiter < 0) {
          field = "body";
          term = token;
        } else {
          field = token.substring(delimiter + 1).toLowerCase();
          term = token.substring(0, delimiter);
        }
        if ((field.compareTo("url") != 0) && (field.compareTo("keywords") != 0) && (field.compareTo("title") != 0)
            && (field.compareTo("body") != 0) && (field.compareTo("inlink") != 0)) {
          throw new IllegalArgumentException("Error: Unknown field " + token);
        }

        // Lexical processing, stopwords, stemming. A loop is used
        // just in case a term (e.g., "near-death") gets tokenized into
        // multiple terms (e.g., "near" and "death").
        String t[] = tokenizeQuery(term);
        if (currentOp instanceof WeightedQry) {
          double weight = weightStack.pop();
          for (int j = 0; j < t.length; j++) {
            Qry termOp = new QryIopTerm(t[j], field);
            currentOp.appendArg(termOp, model);
            ((WeightedQry) currentOp).appendWeight(weight);
          }
          weightExpected = true;
        } else {
          for (int j = 0; j < t.length; j++) {
            Qry termOp = new QryIopTerm(t[j], field);
            currentOp.appendArg(termOp, model);
          }
        }
      }
      // System.out.println("current qry is: " + opStack.peek());
    }

    // A broken structured query can leave unprocessed tokens on the
    // opStack,

    if (tokens.hasMoreTokens()) {
      token = tokens.nextToken();
      // System.out.println(token);
      throw new IllegalArgumentException("Error:  Query syntax is incorrect.  " + qString);
    }

    return currentOp;
  }

  /**
   * Print a message indicating the amount of memory used. The caller can
   * indicate whether garbage collection should be performed, which slows the
   * program but reduces memory usage.
   * 
   * @param gc
   *          If true, run the garbage collector before reporting.
   */
  public static void printMemoryUsage(boolean gc) {

    Runtime runtime = Runtime.getRuntime();

    if (gc)
      runtime.gc();

    System.out.println("Memory used:  " + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
  }

  /**
   * Process one query.
   * 
   * @param qString
   *          A string that contains a query.
   * @param model
   *          The retrieval model determines how matching and scoring is done.
   * @return Search results
   * @throws IOException
   *           Error accessing the index
   */
  static ScoreList processQuery(String qString, RetrievalModel model, Map<String, String> parameters)
      throws IOException {

    Qry q = parseQuery(qString, model, parameters);
    // System.out.println(q);
    q = optimizeQuery(q);
    // System.out.println(q);
    // Show the query that is evaluated

    System.out.println("    --> " + q);

    if (q != null) {

      ScoreList r = new ScoreList();

      if (q.args.size() > 0) { // Ignore empty queries

        q.initialize(model);

        while (q.docIteratorHasMatch(model)) {
          int docid = q.docIteratorGetMatch();
          double score = ((QrySop) q).getScore(model);
          r.add(docid, score);
          q.docIteratorAdvancePast(docid);
        }
      }
      r.sort();
      return r;
    } else
      return null;
  }

  /**
   * Process the query file.
   * 
   * @param queryFilePath
   * @param model
   * @throws Exception 
   */
  static void processQueryFile(Map<String, String> parameters, RetrievalModel model) throws Exception {

    String queryFilePath = parameters.get("queryFilePath");
    String outputPath = parameters.get("trecEvalOutputPath");
    BufferedReader input = null;
    PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
    PrintWriter expansionQueryWriter = null;

    try {
      String qLine = null;

      input = new BufferedReader(new FileReader(queryFilePath));
      
      // for expansion
      boolean fb = "true".equals(parameters.get("fb"));
      String fbInitialRankingFile = parameters.get("fbInitialRankingFile");
      Map<String, ArrayList<ID_Score_Pair>> initialRanking = null;
      if (fb) {
        if (fbInitialRankingFile != null) {
          initialRanking = parseRanking(fbInitialRankingFile);
        } else {
          initialRanking = new HashMap<>();
        }
        expansionQueryWriter = new PrintWriter(parameters.get("fbExpansionQueryFile"), "UTF-8");
      }

      // Each pass of the loop processes one query.

      while ((qLine = input.readLine()) != null) {
        // System.out.println(qLine);
        int d = qLine.indexOf(':');

        if (d < 0) {
          throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
        }

        printMemoryUsage(false);

        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);

        System.out.println("Query " + qLine);

        ScoreList r = null;

        // for expansion
        if (fb) {
          if (fbInitialRankingFile == null) {
            initialRanking.put(qid, getRanking(query, model, parameters));
            // to be continued
          }
          String expandedQuery = getExpandedQuery(qid, initialRanking, parameters);
          writeExpandedQuery(qid, expandedQuery, expansionQueryWriter);
          query = mergeQuery(query, expandedQuery, parameters);
        }
        
        System.out.println(query);
        
        r = processQuery(query, model, parameters);

        if (r != null) {
          printResults(qid, r, writer);
          System.out.println();
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      input.close();
      writer.close();
      if (expansionQueryWriter != null)
        expansionQueryWriter.close();
    }
  }

  /**
   * Print the query results.
   * 
   * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO THAT
   * IT OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
   * 
   * QueryID Q0 DocID Rank Score RunID
   * 
   * @param queryName
   *          Original query.
   * @param result
   *          A list of document ids and scores
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  static void printResults(String queryName, ScoreList result, PrintWriter writer) throws IOException {

    if (result.size() < 1) {
      System.out.println("\tNo results for " + queryName + ".");
      writer.println("\t" + queryName + "\t" + "Q0" + "\t" + "dummy" + "\t" + 1 + "\t" + 0 + "\t" + "run-1");
    } else {
      for (int i = 0; i < result.size() && i < 100; i++) {
        // System.out.println("\t" + queryName + "\t" + "Q0" + "\t" +
        // Idx.getExternalDocid(result.getDocid(i)) + "\t"
        // + (i + 1) + "\t" + result.getDocidScore(i) + "\t" + "run-1");
        writer.println("\t" + queryName + "\t" + "Q0" + "\t" + Idx.getExternalDocid(result.getDocid(i)) + "\t" + (i + 1)
            + "\t" + result.getDocidScore(i) + "\t" + "run-1");
      }
    }
  }

  /**
   * Read the specified parameter file, and confirm that the required parameters
   * are present. The parameters are returned in a HashMap. The caller (or its
   * minions) are responsible for processing them.
   * 
   * @return The parameters, in <key, value> format.
   */
  private static Map<String, String> readParameterFile(String parameterFileName) throws IOException {

    Map<String, String> parameters = new HashMap<String, String>();

    File parameterFile = new File(parameterFileName);

    if (!parameterFile.canRead()) {
      throw new IllegalArgumentException("Can't read " + parameterFileName);
    }

    Scanner scan = new Scanner(parameterFile);
    String line = null;
    do {
      line = scan.nextLine();
      String[] pair = line.split("=");
      parameters.put(pair[0].trim(), pair[1].trim());
    } while (scan.hasNext());

    scan.close();

    if (!(parameters.containsKey("indexPath") && parameters.containsKey("queryFilePath")
        && parameters.containsKey("trecEvalOutputPath") && parameters.containsKey("retrievalAlgorithm"))) {
      throw new IllegalArgumentException("Required parameters were missing from the parameter file.");
    }

    return parameters;
  }

  /**
   * Given a query string, returns the terms one at a time with stopwords
   * removed and the terms stemmed using the Krovetz stemmer.
   * 
   * Use this method to process raw query terms.
   * 
   * @param query
   *          String containing query
   * @return Array of query tokens
   * @throws IOException
   *           Error accessing the Lucene index.
   */
  static String[] tokenizeQuery(String query) throws IOException {

    TokenStreamComponents comp = ANALYZER.createComponents("dummy", new StringReader(query));
    TokenStream tokenStream = comp.getTokenStream();

    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();

    List<String> tokens = new ArrayList<String>();

    while (tokenStream.incrementToken()) {
      String term = charTermAttribute.toString();
      tokens.add(term);
    }

    return tokens.toArray(new String[tokens.size()]);
  }

  /**
   * parse the initial ranking file
   * 
   * @param fbInitialRankingFile
   *          the file path
   * @return a map whose key is the qid, value is also a map, whose key is the
   *         external docid and value is the score for that doc.
   * @throws Exception
   */
  public static Map<String, ArrayList<ID_Score_Pair>> parseRanking(String fbInitialRankingFile) throws Exception {
    Map<String, ArrayList<ID_Score_Pair>> ret = new HashMap<>();
    String curID = null;
    ArrayList<ID_Score_Pair> curArray = null;
    BufferedReader input = null;
    try {
      String line = null;

      input = new BufferedReader(new FileReader(fbInitialRankingFile));

      // Each pass of the loop processes one query.

      while ((line = input.readLine()) != null) {

        if (line.matches("\\s*"))
          continue;

        String[] tokens = line.trim().split("\\s+");
        if (tokens.length != 6)
          throw new IllegalArgumentException();

        String qid = tokens[0];
        String externalDocid = tokens[2];
        double score = Double.parseDouble(tokens[4]);

        if (curID == null || !qid.equals(curID)) {
          curID = qid;
          curArray = new ArrayList<>();
          ret.put(curID, curArray);
        }
        int internalID = Idx.getInternalDocid(externalDocid);
        curArray.add(new ID_Score_Pair(internalID, score));
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      
      input.close();
    }
    //System.out.println(ret.size());
    return ret;
  }

  /**
   * 
   * a class that represents the pair of internal docid and score.
   *
   */
  private static class ID_Score_Pair {
    private int id;
    private double score;

    public ID_Score_Pair(int id, double score) {
      this.id = id;
      this.score = score;
    }

    public int getID() {
      return id;
    }

    public double getScore() {
      return score;
    }
  }

  private static class PairComparatorForIDAndScore implements Comparator<ID_Score_Pair> {
    @Override
    public int compare(ID_Score_Pair o1, ID_Score_Pair o2) {
      double cmp = o1.score - o2.score;
      if (cmp == 0)
        return 0;
      else
        return cmp > 0 ? 1 : -1;
    }
  }
  
/**
 * get the expanded query.
 * @param qid query id
 * @param initialRanking 
 * @param parameters
 * @return 
 * @throws Exception
 */
  private static String getExpandedQuery(String qid, Map<String,ArrayList<ID_Score_Pair>> initialRanking, Map<String, String> parameters) throws Exception {
    ArrayList<ID_Score_Pair> ranking = initialRanking.get(qid);
    int fbTerms = Integer.parseInt(parameters.get("fbTerms"));
    int fbDocs = Integer.parseInt(parameters.get("fbDocs"));
    int fbMu = Integer.parseInt(parameters.get("fbMu"));
    Set<String> candidateTerms = new HashSet<>();
    TermVector[] t = new TermVector[fbDocs];
    ID_Score_Pair[] pairs = new ID_Score_Pair[fbDocs];
    String field = "body"; // assume that.
    // gather all the candidates terms.
    for (int i = 0; i < fbDocs; i++) {
      pairs[i] = ranking.get(i);
      int internalID = pairs[i].getID();
      t[i] = new TermVector(internalID, field);
      int length = t[i].stemsLength();
      for (int j = 0; j < length; j++) {
        String str = t[i].stemString(j);
        if (str != null && !str.contains(".") && !str.contains(","))
          candidateTerms.add(str);
      }
    }
    // calculate scores for these candidates
    PriorityQueue<Term_Score_Pair> queue = new PriorityQueue<>(new PairComparatorForTermAndScore());
    double corpLen = Idx.getSumOfFieldLengths(field);
    for (String term: candidateTerms) {
      double score = 0;
      //System.out.println(term);
      double ctf = Idx.INDEXREADER.totalTermFreq(new Term(field,  term));
      for (int i = 0; i < fbDocs; i++) {
        double docScore = pairs[i].getScore();
        double tf = 0;
        int index = t[i].indexOfStem(term);
        if (index != -1) {
          tf = t[i].stemFreq(index);
//          if (t[i].totalStemFreq(index) != ctf)
//            throw new IllegalStateException();
        }
        double docLen = t[i].positionsLength();
        score += docScore * (tf + fbMu * ctf / corpLen) / (docLen + fbMu);
      }
      score *= Math.log(corpLen / ctf);
      queue.add(new Term_Score_Pair(term, score));
      if (queue.size() > fbTerms)
        queue.poll();
    }
    StringBuilder ret = new StringBuilder();
    ret.append("#wand (");
    while (!queue.isEmpty()) {
      Term_Score_Pair tmp = queue.poll();
      ret.append(" " + tmp.getScore() + " " + tmp.getTerm());
    }
    ret.append(" )");
    return ret.toString();
  }
  /**
   * 
   * pair of candidate term and score
   *
   */
  private static class Term_Score_Pair {
    private String term;
    private double score;

    public Term_Score_Pair(String term, double score) {
      this.term = term;
      this.score = score;
    }

    public String getTerm() {
      return term;
    }

    public double getScore() {
      return score;
    }
  }
  /**
   * 
   * comparator for Term_Score_Pair.
   *
   */
  private static class PairComparatorForTermAndScore implements Comparator<Term_Score_Pair> {
    @Override
    public int compare(Term_Score_Pair o1, Term_Score_Pair o2) {
      double ret = o1.getScore() - o2.getScore();
      if (ret == 0) {
        return 0;
      } else {
        return ret > 0 ? 1 : -1;
      }
    }
  }
  /**
   * merge the original and expanded query.
   * @param query
   * @param expandedQuery
   * @param parameters
   * @return
   */
  public static String mergeQuery(String query, String expandedQuery, Map<String, String> parameters) {
    double w = Double.parseDouble(parameters.get("fbOrigWeight"));
    return "#wand (" + w + " #and(" + query + ") " + (1.0 - w) + " " + expandedQuery + ")";
  }

  private static ArrayList<ID_Score_Pair> getRanking(String qString, RetrievalModel model, Map<String, String> parameters) throws IOException {
    
    Qry q = parseQuery(qString, model, parameters);
    // System.out.println(q);
    q = optimizeQuery(q);
    // System.out.println(q);
    // Show the query that is evaluated

    System.out.println("    --> " + q);

    if (q != null) {
      
      ArrayList<ID_Score_Pair> ret = new ArrayList<>();
      PriorityQueue<ID_Score_Pair> queue = new PriorityQueue<>(new PairComparatorForIDAndScore());

      int fbDocs = Integer.parseInt(parameters.get("fbDocs"));

      if (q.args.size() > 0) { // Ignore empty queries

        q.initialize(model);
        
        while (q.docIteratorHasMatch(model)) {
          int docid = q.docIteratorGetMatch();
          double score = ((QrySop) q).getScore(model);
          queue.add(new ID_Score_Pair(docid, score));
          if (queue.size() > fbDocs)
            queue.poll();
          q.docIteratorAdvancePast(docid);
        }
        while (!queue.isEmpty()) {
          ret.add(0, queue.poll());
        }
      }
      return ret;
    } else
      return null;
  }
  
  private static void writeExpandedQuery(String qid, String expandedQuery, PrintWriter outputFile) {
    outputFile.println(qid + ": " + expandedQuery);
  }
  
}

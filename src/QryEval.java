
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
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
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
    if (model instanceof RetrievalModelLetor) {
      // create feature vectors
      processLetor(parameters, model, true);
      // train the model
      train(parameters);
      // run BM25 to create initial ranking
      double k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
      double b = Double.parseDouble(parameters.get("BM25:b"));
      double k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
      processQueryFile(parameters, new RetrievalModelBM25(k_1, b, k_3));
      // generate feature vectors for initial ranking files and queries
      processLetor(parameters, model, false);
      // create scores for test
      createScoresForTest(parameters);
      rerank(parameters);
    } else
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
    } else if (modelString.equals("letor")) {
      model = new RetrievalModelLetor();
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
      if (input != null)
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
      if (pair.length == 2)
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
      if (input != null)
        input.close();
    }
    // System.out.println(ret.size());
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
   * 
   * @param qid
   *          query id
   * @param initialRanking
   * @param parameters
   * @return
   * @throws Exception
   */
  private static String getExpandedQuery(String qid, Map<String, ArrayList<ID_Score_Pair>> initialRanking,
      Map<String, String> parameters) throws Exception {
    ArrayList<ID_Score_Pair> ranking = initialRanking.get(qid);
    int fbTerms = Integer.parseInt(parameters.get("fbTerms"));
    int fbDocs = Integer.parseInt(parameters.get("fbDocs"));
    int fbMu = Integer.parseInt(parameters.get("fbMu"));
    Set<String> candidateTerms = new HashSet<>();
    TermVector[] t = new TermVector[fbDocs];
    ID_Score_Pair[] pairs = new ID_Score_Pair[fbDocs];
    String field = "body"; // assume that.
    // gather all the candidates terms.
    for (int i = 0; i < fbDocs && i < pairs.length; i++) {
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
    for (String term : candidateTerms) {
      double score = 0;
      // System.out.println(term);
      double ctf = Idx.INDEXREADER.totalTermFreq(new Term(field, term));
      for (int i = 0; i < fbDocs; i++) {
        double docScore = pairs[i].getScore();
        double tf = 0;
        int index = t[i].indexOfStem(term);
        if (index != -1) {
          tf = t[i].stemFreq(index);
          // if (t[i].totalStemFreq(index) != ctf)
          // throw new IllegalStateException();
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
   * 
   * @param query
   * @param expandedQuery
   * @param parameters
   * @return
   */
  public static String mergeQuery(String query, String expandedQuery, Map<String, String> parameters) {
    double w = Double.parseDouble(parameters.get("fbOrigWeight"));
    return "#wand (" + w + " #and(" + query + ") " + (1.0 - w) + " " + expandedQuery + ")";
  }

  private static ArrayList<ID_Score_Pair> getRanking(String qString, RetrievalModel model,
      Map<String, String> parameters) throws IOException {

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

  private static void processLetor(Map<String, String> parameters, RetrievalModel model, boolean training)
      throws Exception {

    PrintWriter featureVectorsFileWriter = null;
    BufferedReader trainingQuery = null;
    // qid -> FeatureVectors
    Map<String, FeatureVectors> fvMap = null;

    try {
      // read relevant document judgements
      Map<String, List<RelevantDoc>> relDocsMap;
      if (training) {
        relDocsMap = readQrelsFile(parameters);
      } else {
        relDocsMap = readInitialRankingFile(parameters);
      }

      // read page rank documents
      Map<String, Double> pageRanksMap = readPageRanks(parameters);

      // generate training data
      String trainingQueryFile;
      if (training)
        trainingQueryFile = parameters.get("letor:trainingQueryFile");
      else
        trainingQueryFile = parameters.get("queryFilePath");
      String qLine = null;
      trainingQuery = new BufferedReader(new FileReader(trainingQueryFile));
      // qid -> FeatureVectors
      fvMap = new HashMap<>();

      while ((qLine = trainingQuery.readLine()) != null) {
        int d = qLine.indexOf(':');
        if (d < 0) {
          throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
        }

        printMemoryUsage(false);

        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);
        String[] terms = tokenizeQuery(query);
        // System.out.println(Arrays.toString(terms));

        System.out.println("Query " + qLine);

        List<RelevantDoc> relDocs = relDocsMap.get(qid);
        if (relDocs == null)
          throw new Exception("qid: " + qid);
        FeatureVectors FV = new FeatureVectors(qid);
        fvMap.put(qid, FV);
        for (RelevantDoc rd : relDocs) {
          String docid = rd.docid;
          int relDegree = rd.relDegree;
          int internalid = Idx.getInternalDocid(docid);
          // not in our index.
          if (internalid == -1)
            continue;
          FV.addDoc(docid);
          // store relevance degree
          FV.setFeatureValue(docid, 0, (double) relDegree);
          // store spam score 1
          double spamScore = Integer.parseInt(Idx.getAttribute("score", internalid));
          FV.setFeatureValue(docid, 1, spamScore);
          // store url depth 2
          String rawUrl = Idx.getAttribute("rawUrl", internalid);
          int count = 0;
          int urlLength = rawUrl.length();
          for (int i = 0; i < urlLength; i++) {
            if (rawUrl.charAt(i) == '/')
              count++;
          }
          // System.out.println(rawUrl + ": " + count);
          FV.setFeatureValue(docid, 2, (double) count);
          // store Wikipedia score 3
          if (rawUrl.contains("wikipedia.org")) {
            FV.setFeatureValue(docid, 3, 1.0);
          } else {
            FV.setFeatureValue(docid, 3, 0.0);
          }
          // store page rank 4
          FV.setFeatureValue(docid, 4, pageRanksMap.get(docid));
          // unique terms percentage in this doc 17
          TermVector tmp = new TermVector(internalid, "body");
          FV.setFeatureValue(docid, 17, ((double) tmp.stemsLength()) / tmp.positionsLength());
          // calculate BM25, Indri and term overlap scores
          for (int i = 0; i < 4; i++) {
            String field = TEXT_FIELDS[i];
            TermVector termVector = new TermVector(internalid, field);
            if (termVector.positionsLength() != 0) {
              double BM25Score = getBM25Score(terms, internalid, termVector, parameters);
              FV.setFeatureValue(docid, 3 * i + 5, BM25Score);
              double indriScore = getIndriScore(terms, internalid, termVector, parameters);
              FV.setFeatureValue(docid, 3 * i + 6, indriScore);
              double overlapScore = getOverlapScore(terms, termVector);
              FV.setFeatureValue(docid, 3 * i + 7, overlapScore);
              if (i == 0) {
                double overlapScore2 = getOverlapScoreInDoc(terms, termVector);
                FV.setFeatureValue(docid, 18, overlapScore2);
              }
            } else {
              // System.out.println(docid);
              FV.setFeatureValue(docid, 3 * i + 5, null);
              FV.setFeatureValue(docid, 3 * i + 6, null);
              FV.setFeatureValue(docid, 3 * i + 7, null);
            }
          }
          // ~~~~~~~ 17 18 feature to be implemented ~~~~~~~~~
        }
        // normalize feature values
        normalize(FV);

      }

      // disabled features
      Set<Integer> disabled = new HashSet<>();
      String fd = parameters.get("letor:featureDisable");
      if (fd != null) {
        String[] numbers = fd.trim().split(",");
        for (String number : numbers)
          disabled.add(Integer.parseInt(number));
      }

      ArrayList<String> qids = new ArrayList<>(fvMap.keySet());
      Collections.sort(qids);
      if (training)
        featureVectorsFileWriter = new PrintWriter(parameters.get("letor:trainingFeatureVectorsFile"), "UTF8");
      else
        featureVectorsFileWriter = new PrintWriter(parameters.get("letor:testingFeatureVectorsFile"), "UTF8");
      for (String qid : qids) {
        FeatureVectors fv = fvMap.get(qid);
        Iterable<String> docids = fv.docids();
        for (String docid : docids) {
          featureVectorsFileWriter.print(fv.getValue(docid, 0) + " qid:" + fv.getQid() + " ");
          for (int i = 1; i < 19; i++) {
            if (disabled.contains(i))
              continue;
            featureVectorsFileWriter.print(i + ":" + fv.getValue(docid, i) + " ");
          }
          featureVectorsFileWriter.print("# " + docid + "\n");
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (trainingQuery != null)
        trainingQuery.close();
      if (featureVectorsFileWriter != null)
        featureVectorsFileWriter.close();
    }

  }

  private static class RelevantDoc {
    private String docid;
    private int relDegree;

    public RelevantDoc(String s, int i) {
      docid = s;
      relDegree = i;
    }
  }

  private static void normalize(FeatureVectors fv) {
    // initialize max and min values
    double[] maxValue = new double[19];
    double[] minValue = new double[19];
    Arrays.fill(minValue, Double.POSITIVE_INFINITY);
    Arrays.fill(maxValue, Double.NEGATIVE_INFINITY);
    maxValue[0] = 2;
    minValue[0] = 0;
    Iterable<String> docids = fv.docids();
    for (String docid : docids) {
      for (int i = 1; i < 19; i++) {
        Double tmp = fv.getValue(docid, i);
        if (tmp == null)
          continue;
        if (tmp > maxValue[i])
          maxValue[i] = tmp;
        if (tmp < minValue[i])
          minValue[i] = tmp;
      }
    }
    // System.out.println(Arrays.toString(maxValue));
    // System.out.println(Arrays.toString(minValue));
    // normalization
    for (String docid : docids) {
      for (int i = 1; i < 19; i++) {
        Double value = fv.getValue(docid, i);
        double normalizedValue = value == null ? 0
            : (maxValue[i] - minValue[i]) == 0 ? 0 : (value - minValue[i]) / ((maxValue[i] - minValue[i]));
        fv.setFeatureValue(docid, i, normalizedValue);
      }
    }
  }

  private static double getBM25Score(String[] terms, int docid, TermVector termVector, Map<String, String> parameters)
      throws IOException {

    double score = 0;
    double k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
    double b = Double.parseDouble(parameters.get("BM25:b"));
    double k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
    double docLen = termVector.positionsLength();
    double avg_docLen = ((double) Idx.getSumOfFieldLengths(termVector.fieldName))
        / Idx.getDocCount(termVector.fieldName);

    for (String term : terms) {
      // calculate idf
      int termIndex = termVector.indexOfStem(term);
      if (termIndex == -1)
        continue;
      double df = termVector.stemDf(termIndex);
      // double df = Idx.INDEXREADER.docFreq(new Term(term));
      double numerator = Idx.getNumDocs() - df + 0.5;
      double denominator = df + 0.5;
      double idf = Math.max(0, Math.log(numerator / denominator));
      // calculate tfWeight
      double tf;
      tf = termVector.stemFreq(termIndex);
      double temp = tf + k_1 * ((1 - b) + b * docLen / avg_docLen);
      double tfWeight = tf / temp;
      // calculate userWeight
      double qtf = 1; // not sure ?
      double userWeight = (k_3 + 1) * qtf / (k_3 + qtf);
      // if (Idx.getExternalDocid(docid).equals("clueweb09-en0000-95-28647"))
      // System.out.println("BM25: " + term + ": tf " + tf + " docLen " +
      // docLen);
      score += idf * tfWeight * userWeight;
    }

    return score;
  }

  private static double getIndriScore(String[] terms, int docid, TermVector termVector, Map<String, String> parameters)
      throws IOException {
    // check whether there is one term in the doc, if no, return 0
    boolean present = false;
    for (String term : terms) {
      int index = termVector.indexOfStem(term);
      if (index != -1)
        present = true;
    }
    if (!present)
      return 0.0;
    double score = 1.0;
    double corpLen = Idx.getSumOfFieldLengths(termVector.fieldName);
    double docLen = termVector.positionsLength();
    double mu = Double.parseDouble(parameters.get("Indri:mu"));
    double lambda = Double.parseDouble(parameters.get("Indri:lambda"));
    double power = 1.0 / terms.length;
    for (String term : terms) {
      int index = termVector.indexOfStem(term);
      double tf = index == -1 ? 0 : termVector.stemFreq(index);
      double ctf = Idx.INDEXREADER.totalTermFreq(new Term(termVector.fieldName, new BytesRef(term)));
      // if (Idx.getExternalDocid(docid).equals("clueweb09-en0000-95-28647"))
      // System.out.println(termVector.fieldName + ": " + term + ": tf " + tf +
      // " ctf " + ctf +
      // " corpLen " + corpLen + " docLen " + docLen + " mu " + mu + " lambda "
      // + lambda);
      score *= Math.pow((1 - lambda) * ((tf + mu * ctf / corpLen) / (docLen + mu)) + lambda * ctf / corpLen, power);
      // if (Idx.getExternalDocid(docid).equals("clueweb09-en0000-95-28647"))
      // System.out.println("score: " + score);
    }
    return score;
  }

  private static double getOverlapScore(String[] terms, TermVector termVector) throws IOException {
    double score = 0;
    for (String term : terms) {
      int index = termVector.indexOfStem(term);
      if (index != -1)
        score++;
    }
    return score / terms.length;
  }

  private static double getOverlapScoreInDoc(String[] terms, TermVector termVector) {
    double count = 0;
    for (String term : terms) {
      int index = termVector.indexOfStem(term);
      if (index != -1)
        count += termVector.stemFreq(index);
    }
    return count / termVector.positionsLength();
  }

  private static Map<String, List<RelevantDoc>> readQrelsFile(Map<String, String> parameters)
      throws NumberFormatException, IOException {
    // read relevance judgements.
    BufferedReader judgements = null;
    Map<String, List<RelevantDoc>> relDocsMap = new HashMap<>();
    try {
      String qrelsFile = parameters.get("letor:trainingQrelsFile");
      judgements = new BufferedReader(new FileReader(qrelsFile));
      String rline = null;
      while ((rline = judgements.readLine()) != null) {
        String[] tokens = rline.split("\\s+");
        if (tokens.length != 4)
          throw new IllegalArgumentException(Arrays.toString(tokens));
        String qid = tokens[0];
        String docid = tokens[2];
        int relDegree = Integer.parseInt(tokens[3]);
        if (relDocsMap.get(qid) == null)
          relDocsMap.put(qid, new ArrayList<RelevantDoc>());
        relDocsMap.get(qid).add(new RelevantDoc(docid, relDegree));
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (judgements != null)
        judgements.close();
    }
    return relDocsMap;
  }

  private static Map<String, List<RelevantDoc>> readInitialRankingFile(Map<String, String> parameters)
      throws NumberFormatException, IOException {
    // read relevance judgements.
    BufferedReader initialRanking = null;
    Map<String, List<RelevantDoc>> relDocsMap = new HashMap<>();
    try {
      String initialRankingFile = parameters.get("trecEvalOutputPath");
      initialRanking = new BufferedReader(new FileReader(initialRankingFile));
      String rline = null;
      while ((rline = initialRanking.readLine()) != null) {
        String[] tokens = rline.trim().split("\\s+");
        if (tokens.length != 6)
          throw new IllegalArgumentException("length: " + tokens.length + Arrays.toString(tokens));
        String qid = tokens[0];
        String docid = tokens[2];
        if (relDocsMap.get(qid) == null)
          relDocsMap.put(qid, new ArrayList<RelevantDoc>());
        relDocsMap.get(qid).add(new RelevantDoc(docid, 0));
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (initialRanking != null)
        initialRanking.close();
    }
    return relDocsMap;
  }

  private static Map<String, Double> readPageRanks(Map<String, String> parameters) throws IOException {
    // read page rank documents
    BufferedReader pageRanks = null;
    Map<String, Double> pageRanksMap = new HashMap<>();
    String pline = null;
    try {
      String pageRankFile = parameters.get("letor:pageRankFile");
      pageRanks = new BufferedReader(new FileReader(pageRankFile));
      while ((pline = pageRanks.readLine()) != null) {
        String[] tokens = pline.split("\\s+");
        if (tokens.length != 2)
          throw new IllegalArgumentException();
        String docid = tokens[0];
        double docValue = Double.parseDouble(tokens[1]);
        pageRanksMap.put(docid, docValue);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (pageRanks != null)
        pageRanks.close();
    }
    return pageRanksMap;
  }

  private static void train(Map<String, String> parameters) throws Exception {
    // _______________________ train_____________________________
    // runs svm_rank_learn from within Java to train the model
    // execPath is the location of the svm_rank_learn utility,
    // which is specified by letor:svmRankLearnPath in the parameter file.
    // FEAT_GEN.c is the value of the letor:c parameter.
    Process cmdProc = Runtime.getRuntime()
        .exec(new String[] { parameters.get("letor:svmRankLearnPath"), "-c",
            String.valueOf(parameters.get("letor:svmRankParamC")), parameters.get("letor:trainingFeatureVectorsFile"),
            parameters.get("letor:svmRankModelFile") });

    // The stdout/stderr consuming code MUST be included.
    // It prevents the OS from running out of output buffer space and
    // stalling.

    // consume stdout and print it out for debugging purposes
    BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(cmdProc.getInputStream()));
    String line;
    while ((line = stdoutReader.readLine()) != null) {
      System.out.println(line);
    }
    // consume stderr and print it for debugging purposes
    BufferedReader stderrReader = new BufferedReader(new InputStreamReader(cmdProc.getErrorStream()));
    while ((line = stderrReader.readLine()) != null) {
      System.out.println(line);
    }

    // get the return value from the executable. 0 means success, non-zero
    // indicates a problem
    int retValue = cmdProc.waitFor();
    if (retValue != 0) {
      throw new Exception("SVM Rank crashed.");
    }
    // __________________________________________________________
  }

  private static void createScoresForTest(Map<String, String> parameters) throws Exception {
    // _______________________ train_____________________________
    // runs svm_rank_learn from within Java to train the model
    // execPath is the location of the svm_rank_learn utility,
    // which is specified by letor:svmRankLearnPath in the parameter file.
    // FEAT_GEN.c is the value of the letor:c parameter.
    Process cmdProc = Runtime.getRuntime()
        .exec(new String[] { parameters.get("letor:svmRankClassifyPath"),
            parameters.get("letor:testingFeatureVectorsFile"), parameters.get("letor:svmRankModelFile"),
            parameters.get("letor:testingDocumentScores") });

    // The stdout/stderr consuming code MUST be included.
    // It prevents the OS from running out of output buffer space and
    // stalling.

    // consume stdout and print it out for debugging purposes
    BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(cmdProc.getInputStream()));
    String line;
    while ((line = stdoutReader.readLine()) != null) {
      System.out.println(line);
    }
    // consume stderr and print it for debugging purposes
    BufferedReader stderrReader = new BufferedReader(new InputStreamReader(cmdProc.getErrorStream()));
    while ((line = stderrReader.readLine()) != null) {
      System.out.println(line);
    }

    // get the return value from the executable. 0 means success, non-zero
    // indicates a problem
    int retValue = cmdProc.waitFor();
    if (retValue != 0) {
      throw new Exception("SVM Rank crashed.");
    }
    // __________________________________________________________
  }

  private static void rerank(Map<String, String> parameters) throws IOException {
    BufferedReader initialRanking = null;
    BufferedReader docScore = null;
    LinkedHashMap<String, List<DocScore>> idsMap = new LinkedHashMap<>();
    List<Double> scores = new ArrayList<>();
    PrintWriter writer = null;
    try {
      String docScoreFile = parameters.get("letor:testingDocumentScores");
      docScore = new BufferedReader(new FileReader(docScoreFile));
      String sline = null;
      while ((sline = docScore.readLine()) != null) {
        scores.add(Double.parseDouble(sline));
      }

      int i = 0; // index of docScore list
      String initialRankingFile = parameters.get("trecEvalOutputPath");
      initialRanking = new BufferedReader(new FileReader(initialRankingFile));
      String rline = null;
      while ((rline = initialRanking.readLine()) != null) {
        String[] tokens = rline.trim().split("\\s+");
        if (tokens.length != 6)
          throw new IllegalArgumentException("length: " + tokens.length + Arrays.toString(tokens));
        String qid = tokens[0];
        String docid = tokens[2];
        if (idsMap.get(qid) == null)
          idsMap.put(qid, new ArrayList<DocScore>());
        List<DocScore> ids = idsMap.get(qid);
        ids.add(new DocScore(docid, scores.get(i++)));
      }
      if (i != scores.size())
        throw new IllegalStateException();

      for (String qid : idsMap.keySet()) {
        idsMap.get(qid)
            .sort((DocScore d1, DocScore d2) -> (d1.score - d2.score == 0 ? 0 : d1.score - d2.score < 0 ? 1 : -1));
      }

      writer = new PrintWriter(parameters.get("trecEvalOutputPath"), "UTF8");
      for (String qid : idsMap.keySet()) {
        List<DocScore> scoreList = idsMap.get(qid);
        int k = 0;
        for (DocScore d : scoreList) {
          writer.println("\t" + qid + "\t" + "Q0" + "\t" + d.id + "\t" + (++k) + "\t" + d.score + "\t" + "yubinletor");
        }
      }

    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (initialRanking != null)
        initialRanking.close();
      if (docScore != null)
        docScore.close();
      if (writer != null)
        writer.close();
    }

  }

  private static class DocScore {
    private String id;
    private double score;

    public DocScore(String i, double s) {
      id = i;
      score = s;
    }
  }

}

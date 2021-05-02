package mypackage;

import org.rocksdb.*;

import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import static java.util.Collections.reverseOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Status;

public class InvertedIndex {
	private RocksDB docmapdb;
	private RocksDB wordmapdb;
	private RocksDB inverteddb;
	private RocksDB forwarddb;
	private RocksDB metadatadb;
	private RocksDB parentchilddb;
	private RocksDB pagerankdb;

	private Options options;

	private String main_path = "/Users/anthonykwok/Documents/Academic/HKUST/Year 2020-2021 (DSCT Yr 3)/2021 Spring Semester Course/COMP4321/Project/4321-repo/apache-tomcat-10.0.5/webapps/example/";
	private String db1 = main_path + "db/doc";
	private String db2 = main_path + "db/word";
	private String db3 = main_path + "db/invert";
	private String db4 = main_path + "db/forward";
	private String db5 = main_path + "db/metadata";
	private String db6 = main_path + "db/parentchild";
	private String db7 = main_path + "db/pagerank";

	private int wordNextID;
	private int docNextID;
	private int prPrecision;
	private double thersold;

	public InvertedIndex() throws RocksDBException {
		RocksDB.loadLibrary();

		this.options = new Options();
		this.options.setCreateIfMissing(true);

		this.wordNextID = 0;
		this.docNextID = 0;
		this.prPrecision = 7;
		this.thersold = 1 / power(prPrecision);
	}

	public String sayHaHa(String query) throws RocksDBException {
		return "No Error" + query;
	}

	// ========== Data Security ==========

	// Open all database connection
	public void openAllDB() throws RocksDBException {
		try {
			this.docmapdb = RocksDB.open(this.options, this.db1);
			this.wordmapdb = RocksDB.open(this.options, this.db2);
			this.inverteddb = RocksDB.open(this.options, this.db3);
			this.forwarddb = RocksDB.open(this.options, this.db4);
			this.metadatadb = RocksDB.open(this.options, this.db5);
			this.parentchilddb = RocksDB.open(this.options, this.db6);
			this.pagerankdb = RocksDB.open(this.options, this.db7);
		} catch (RocksDBException e) {
			System.err.println(e.toString());
		}
	}

	// Close all database connection
	public void closeAllDB() {
		this.docmapdb.close();
		this.wordmapdb.close();
		this.inverteddb.close();
		this.forwarddb.close();
		this.metadatadb.close();
		this.parentchilddb.close();
		this.pagerankdb.close();
	}

	// ========== Document Mapping Part ==========

	// Add new Document Mapping pair (e.g. "https://www.cse.ust.hk/" : "doc0")
	public void addDocMappingEntry(String url) throws RocksDBException {
		byte[] content = docmapdb.get(url.getBytes());
		if (content != null) {
			// Stop adding if mapping exists
			return;
		} else {
			// Add new doc mapping
			content = ("doc" + this.docNextID).getBytes();
			this.docNextID++;
		}
		docmapdb.put(url.getBytes(), content);
	}

	// Get all existing Document ID (e.g. "doc1 doc5 doc2")
	public String getAllDocID() throws RocksDBException {
		String docIDList = "";
		RocksIterator iter = docmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			docIDList = docIDList + new String(iter.value()) + " ";
		}
		iter.close();
		return docIDList;
	}

	// Get the Number of Document in db
	public int getNumOfDoc() throws RocksDBException {
		String docIDList = getAllDocID();
		return docIDList.split(" ").length;
	}

	// Get the Doc ID by the URL (e.g. "https://www.cse.ust.hk/" returns "doc0")
	public String getDocIDbyURL(String url) throws RocksDBException {
		String result = "";
		byte[] content = docmapdb.get(url.getBytes());
		if (content != null) {
			// if docID exist
			result = new String(content);
		} else {
			// if docID not exist, make one
			addDocMappingEntry(url);
			result = getDocIDbyURL(url);
		}
		return result;
	}

	// Get the URL by the Doc ID (e.g. "doc0" returns "https://www.cse.ust.hk/")
	public String getURLbyDocID(String docID) throws RocksDBException {
		RocksIterator iter = docmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			String value = new String(iter.value());
			if (value.equals(docID)) {
				return new String(iter.key());
			}
		}
		iter.close();
		return "";
	}

	// Delete Document Mapping by URL in RocksDB
	public void delDocMapbyURL(String url) throws RocksDBException {
		docmapdb.remove(url.getBytes());
	}

	// Delete Document Mapping by URL
	public void delDocMapbyDocID(String docID) throws RocksDBException {
		String url = getURLbyDocID(docID);
		delDocMapbyURL(url);
	}

	// Delete All Document Mapping
	public void delAllDocMap() throws RocksDBException {
		System.out.println(">>> Deleting All Doc Mapping...");
		RocksIterator iter = docmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			docmapdb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print out All Document Mapping pairs
	public void printAllDocMapping() throws RocksDBException {
		System.out.println(">>> Printing All Doc Mapping...");
		RocksIterator iter = docmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// ========== Word Mapping Part ==========

	// Add new word mapping pair (e.g. "abd": "word1")
	public void addWordMappingEntry(String word) throws RocksDBException {
		byte[] content = wordmapdb.get(word.getBytes());
		if (content != null) {
			// Stop adding if mapping exists
			return;
		} else {
			// Add new word mapping
			content = ("word" + this.wordNextID).getBytes();
			this.wordNextID++;
		}
		wordmapdb.put(word.getBytes(), content);
	}

	// Get all existing Word ID (e.g. "word1 word5 word2")
	public String getAllWordID() throws RocksDBException {
		RocksIterator iter = wordmapdb.newIterator();
		String docIDList = "";
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			docIDList = docIDList + new String(iter.value()) + " ";
		}
		iter.close();
		return docIDList;
	}

	// Get the WordID by the Word (e.g. "abd" => "word1")
	public String getWordIDbyWord(String word) throws RocksDBException {
		byte[] content = wordmapdb.get(word.getBytes());
		if (content != null) {
			return new String(content);
		}
		return "";
	}

	// Get the Word by the Word ID (e.g. "word1" returns "abd")
	public String getWordbyWordID(String wordID) throws RocksDBException {
		RocksIterator iter = wordmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			if (new String(iter.value()).equals(wordID)) {
				return new String(iter.key());
			}
		}
		iter.close();
		return "";
	}

	// Delete Word Mapping by Word in the Word Mapping
	public void delWordMapbyWord(String word) throws RocksDBException {
		wordmapdb.remove(word.getBytes());
	}

	// Delete Word Mapping by WordID in the Word Mapping
	public void delWordMapbyWordID(String wordID) throws RocksDBException {
		String word = getWordbyWordID(wordID);
		wordmapdb.remove(word.getBytes());
	}

	// Delete All Word Mapping
	public void delAllWordMap() throws RocksDBException {
		System.out.println(">>> Deleting All Word Mapping...");
		RocksIterator iter = wordmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			wordmapdb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print out All Word Mapping pairs
	public void printAllWordMapping() throws RocksDBException {
		System.out.println(">>> Printing All Word Mapping...");
		RocksIterator iter = wordmapdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// ========== Metadata Part ==========

	// Add Metadata of the URL into RocksDB ("doc1" :
	// "title:lastmodification:size:lang")
	public void addMetadata(String url, String title, String lm, int size, String lang, int level)
			throws RocksDBException {
		String docID = getDocIDbyURL(url);
		// Overwrite the old data if crawl again
		byte[] content = (title + ":" + lm + ":" + size + ":" + lang).getBytes();
		metadatadb.put(docID.getBytes(), content);
	}

	// Get all metadata of the URL
	public Vector<String> getMetadata(String url) throws RocksDBException {
		Vector<String> result = new Vector<String>();
		String docID = getDocIDbyURL(url);
		byte[] content = metadatadb.get(docID.getBytes());
		if (content != null) {
			String data = new String(content);
			for (String d : data.split(":")) {
				result.add(d);
			}
		}
		return result;
	}

	// Get Title of the URL
	public String getTitlebyURL(String url) throws RocksDBException {
		Vector<String> meta = getMetadata(url);
		if (meta.size() != 0) {
			return meta.get(0);
		}
		return "";
	}

	// Get Last Modification Date of the URL
	public String getLastModifiedbyURL(String url) throws RocksDBException {
		Vector<String> meta = getMetadata(url);
		if (meta.size() != 0) {
			return meta.get(1);
		}
		return "";
	}

	// Get Size of the URL
	public String getSizebyURL(String url) throws RocksDBException {
		Vector<String> meta = getMetadata(url);
		if (meta.size() != 0) {
			return meta.get(2);
		}
		return "";
	}

	// Get Lang of the URL
	public String getLangbyURL(String url) throws RocksDBException {
		Vector<String> meta = getMetadata(url);
		if (meta.size() != 0) {
			return meta.get(3);
		}
		return "";
	}

	// Delete All Metadata
	public void delAllMetadata() throws RocksDBException {
		System.out.println(">>> Deleting All Metadata...");
		RocksIterator iter = metadatadb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			metadatadb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print out All Metadata
	public void printAllMetadata() throws RocksDBException {
		System.out.println(">>> Printing All Metadata...");
		RocksIterator iter = metadatadb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// Print out All Metadata by Doc ID
	public void printAllMetadataByDocID(String docID) throws RocksDBException {
		System.out.println(">>> Printing out Metadata by Doc ID...");
		byte[] content = metadatadb.get(docID.getBytes());
		if (content != null) {
			System.out.println(new String(content));
		}
	}

	// ========== Inverted Index Part ==========

	// Add word into inverted index (e.g. "word1" : "doc0:2 doc2:5"

	public void addInvertedIndex(String url, String word, int freq) throws RocksDBException {
		String docID = getDocIDbyURL(url);
		String wordID = getWordIDbyWord(word);
		byte[] content = inverteddb.get(wordID.getBytes());
		if (content != null) {
			boolean updated = false;
			String update_freq = "";
			String[] docFreq_list = new String(content).split(" ");
			for (String docFreqPair : docFreq_list) {
				if (!docFreqPair.contains(":")) {
					continue;
				}
				String[] pair = docFreqPair.split(":");
				String d = pair[0];
				String f = pair[1];
				// Check if the record exists
				if (d.equals(docID)) {
					// Need Update
					update_freq = update_freq + docID + ":" + String.valueOf(freq) + " ";
					updated = true;
				} else {
					// No need update
					update_freq = update_freq + d + ":" + f + " ";
				}
			}
			// If have not update, append it
			if (!updated) {
				update_freq = update_freq + docID + ":" + String.valueOf(freq) + " ";
			}
			content = (update_freq).getBytes();
		} else {
			// Add the new inverted indexing for the wordID
			content = (docID + ":" + freq).getBytes();
		}
		inverteddb.put(wordID.getBytes(), content);
	}

	// Get inverted index posting of a word (e.g. input "word1" to get "doc0:2
	// doc2:5")
	public String getInvertedIndexByWord(String word) throws RocksDBException {
		String wordID = getWordIDbyWord(word);
		if (wordID.equals("")) {
			return "empty";
		}
		byte[] content = inverteddb.get(wordID.getBytes());
		String data = "";
		if (content != null) {
			data = new String(content);
		} else
			data = "empty";
		return data;
	}

	// Get frequency of term (word) in document (docID)
	public int getTF(String word, String docID) throws RocksDBException {
		String invind = this.getForwardByDocID(docID);
		if (invind.equals("empty")) {
			return 0;
		}
		invind = invind.substring(invind.indexOf(word));
		invind = invind.substring(invind.indexOf(":") + 1, invind.indexOf(" "));
		return Integer.parseInt(invind);
	}

	// Get doc frequency of term (word)
	public int calculateDocumentFrequencyOfTerm(String word) throws RocksDBException {
		String invind = getInvertedIndexByWord(word);
		String[] invind_split = invind.split(" ");
		return invind_split.length;
	}

	// Get the inverse document frequency of term (word)
	public double calculateIDF(String word, String docID, int N) throws RocksDBException {
		double idf = (Math.log(N / calculateDocumentFrequencyOfTerm(word)) / Math.log(2));
		return idf;
	}

	// Calculate Cosine Similarity
	public double calculateCosineSimilarity(String docID, double termWeight, int queryLength) throws RocksDBException {
		double innerProduct = termWeight;
		double documentLength = calculateDocumentLengthByDocID(docID);
		double cosSim = termWeight / Math.sqrt(documentLength);
		return cosSim;
	}

	// return ranking based on cosine similarity
	public Map<String, Double> rankingAlgorithm(String query) throws RocksDBException {
		// docID, score key-value pair for ranking
		int N = this.getNumOfDoc();

		Map<String, Double> ranking;
		ranking = new HashMap<String, Double>();

		// split query by space into array eg) hello, world
		String[] querySplit = query.split(" ");

		// getInvertedIndex for all queries in a for loop
		for (int i = 0; i < querySplit.length; i++) {
			String queryInvInd = this.getInvertedIndexByWord(querySplit[i]);
			// world -> doc0:2 doc16:1
			if (!queryInvInd.equals("empty")) {
				String[] invIndSplit = queryInvInd.split(" ");
				// world -> [doc0:2, doc16:1]
				for (int j = 0; j < invIndSplit.length; j++) {
					// s = docID, just the number, doc that has at least a word in query
					String s = invIndSplit[j].substring(invIndSplit[j].indexOf("c") + 1);
					s = s.substring(0, s.indexOf(":"));

					// create docID key if not exist
					if (!ranking.containsKey(s))
						ranking.put(s, 0.0);

					// find doc's cosine similarity with query and put into ranking array
					String forwardList = this.getForwardByDocID(s);
					if (forwardList.contains(querySplit[i])) {
						// calculate term weight of all document terms here

						// inner product with each word in query
						double termFreq = getTF(querySplit[i], s);
						double idf = calculateIDF(querySplit[i], s, N);
						double[] maxTFAndDocLength = calculateDocVectorLength(s, forwardList, N);
						double maxTF = maxTFAndDocLength[0];
						double termWeight = termFreq * idf / maxTF;

						// divide by |doc| and put cosSim into ranking
						double docLength = maxTFAndDocLength[1];
						double cosSim = termWeight / docLength;
						ranking.put(s, ranking.get(s) + cosSim);
					}

				}
			}
		}
		// finally sort the hashmap and return for rendering
		Map<String, Double> sortedRanking = ranking.entrySet().stream()
				.sorted(reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

		// print to console for debugging
		sortedRanking.entrySet().forEach(entry -> {
			System.out.println("docID = " + entry.getKey() + " score = " + entry.getValue());
		});
		return sortedRanking;
	}

	// calculate number of words (without uniqueness) for normalization
	public double[] calculateDocVectorLength(String docID, String forwardList, int N) throws RocksDBException {
		String[] wordNFreq = forwardList.split(" ");
		double[] termWeights = new double[wordNFreq.length];
		int maxTF = -100;
		int index = 0;
		// word:freq, calculate termWeight without maxTF normalization first
		for (String pair : wordNFreq) {
			String word = pair.substring(0, pair.lastIndexOf(":"));
			int freq = Integer.parseInt(pair.substring(pair.lastIndexOf(":") + 1));
			if (freq > maxTF)
				maxTF = freq;
			double idf = calculateIDF(word, docID, N);
			double termWeight = freq * idf;
			termWeights[index++] = termWeight;
		}
		// normalize and square all termWeights by maxTF
		for (double weight : termWeights) {
			weight /= maxTF;
			weight *= weight;
		}
		double docLength = Math.sqrt(Arrays.stream(termWeights).sum());
		double[] maxTFAndDocLength = new double[2];
		maxTFAndDocLength[0] = maxTF;
		maxTFAndDocLength[1] = docLength;
		return maxTFAndDocLength;
	}

	// Delete All Inverted Index
	public void delAllInvertedIndex() throws RocksDBException {
		System.out.println(">>> Deleting All Inverted Index...");
		RocksIterator iter = inverteddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			inverteddb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print out All Inverted Index

	public void printAllInvertedIndex() throws RocksDBException {
		System.out.println(">>> Printing All Inverted Index...");
		RocksIterator iter = inverteddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// Print out All Inverted Index by Word ID
	public void printAllInvertedIndex(String wordID) throws RocksDBException {
		System.out.println(">>> Printing out Inverted Index by Word ID...");
		byte[] content = inverteddb.get(wordID.getBytes());
		if (content != null) {
			System.out.println(new String(content));
		}
	}

	// ========== Forward Index Part ==========

	// Add data into forward index (e.g. "doc1" : "word4:2 word5:1")
	public void addForwardIndex(String url, String word, int count) throws RocksDBException {
		String docID = getDocIDbyURL(url);
		byte[] content = forwarddb.get(docID.getBytes());
		if (content != null) {
			boolean updated = false;
			String update_freq = "";
			String[] wordFreq_list = new String(content).split(" ");
			for (String wordFreqPair : wordFreq_list) {
				String[] pair = wordFreqPair.split(":");
				String w = pair[0];
				String f = pair[1];
				// Check if the record exists
				if (w.equals(word)) {
					// Need Update
					update_freq = update_freq + word + ":" + String.valueOf(count) + " ";
					updated = true;
				} else {
					// No need update
					update_freq = update_freq + w + ":" + f + " ";
				}
			}
			// If have not update, append it
			if (!updated) {
				update_freq = update_freq + word + ":" + String.valueOf(count) + " ";
			}
			content = (update_freq).getBytes();
		} else {
			// Add new forward indexing for the docID
			content = (word + ":" + String.valueOf(count)).getBytes();
		}
		forwarddb.put(docID.getBytes(), content);
	}

	// Get data in forward index by URL
	public String getForwardIndex(String url) throws RocksDBException {
		String docID = getDocIDbyURL(url);
		byte[] content = forwarddb.get(docID.getBytes());
		String result = "";
		if (content != null) {
			String wordFreq = new String(content);
			String[] wordFreqPair = wordFreq.split(" ");
			for (String pair : wordFreqPair) {
				if (!pair.contains(":")) {
					continue;
				}
				String keyword = pair.split(":")[0];
				String freq = pair.split(":")[1];
				result = result + keyword + " " + freq + "; ";
			}
		}
		return result + "\n";
	}

	// Get data in forward index by docID (just the number)
	public String getForwardByDocID(String docID) throws RocksDBException {
		String str = "doc" + docID;
		byte[] content = forwarddb.get(str.getBytes());
		String result = "";
		if (content != null) {
			result = new String(content);
		} else
			result = "empty";
		return result;
	}

	// calculate number of words (without uniqueness) for normalization
	public int calculateDocumentLengthByDocID(String docID) throws RocksDBException {
		int length = 0;
		String forward_docID = getForwardByDocID(docID);
		if (!forward_docID.equals("empty")) {
			String[] word_and_freq_split = forward_docID.split(" ");
			for (String wordnfreq : word_and_freq_split) {
				int individual_freq = Integer.parseInt(wordnfreq.substring(wordnfreq.lastIndexOf(":") + 1));
				length += individual_freq;
			}
		}
		return length;
	}

	// Delete All Forward Index
	public void delAllForwardIndex() throws RocksDBException {
		System.out.println(">>> Deleting All Forward Index...");
		RocksIterator iter = forwarddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			forwarddb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print out All Forward Index
	public void printAllForwardIndex() throws RocksDBException {
		System.out.println(">>> Printing All Forward Index...");
		RocksIterator iter = forwarddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// Print out All Forward Index by Doc ID
	public void printAllForwardIndexbyDocID(String docID) throws RocksDBException {
		System.out.println(">>> Printing out Forward Index by Doc ID...");
		byte[] content = forwarddb.get(docID.getBytes());
		if (content != null) {
			System.out.println(new String(content));
		}
	}

	// ========== Linkage Part (Parent-Child Relation) ==========

	// Add the link between the parent and child relation
	public void addPCRelation(String p_url, String c_url) throws RocksDBException {
		String parentID = getDocIDbyURL(p_url);
		String childID = getDocIDbyURL(c_url);
		byte[] content = parentchilddb.get(parentID.getBytes());
		if (content != null) {
			String old_children = new String(content);
			if (!old_children.contains(childID)) {
				content = (new String(content) + " " + childID).getBytes();
			}

		} else {
			content = (childID).getBytes();
		}
		parentchilddb.put(parentID.getBytes(), content);
	}

	// Get the parent child relation by the parent URL
	public String getPCRelation(String p_url) throws RocksDBException {
		String result = new String();
		String parentID = getDocIDbyURL(p_url);
		byte[] content = parentchilddb.get(parentID.getBytes());
		if (content != null) {
			String new_content = new String(content);
			String[] children = new_content.split(" ");
			int count = 0;
			for (String child : children) {
				count++;
				result = result + "Child Link " + String.valueOf(count) + ": " + getURLbyDocID(child) + "\n";
			}
			return result;
		} else {
			return "";
		}
	}

	// Print out All Parent Child Relation
	public void printAllParentChild() throws RocksDBException {
		System.out.println(">>> Printing All Parent Child Relation...");
		RocksIterator iter = parentchilddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// Print out All Parent Child Relation
	public void delAllParentChild() throws RocksDBException {
		System.out.println(">>> Deleting All Parent Child Relation...");
		RocksIterator iter = parentchilddb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			parentchilddb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Check if the doc ID has the child doc ID
	public boolean checkIfChildExist(String p_doc, String c_doc) throws RocksDBException {
		byte[] content = parentchilddb.get(p_doc.getBytes());
		if (content != null) {
			String children = new String(content);
			if (children.contains(c_doc)) {
				return true;
			}
		}
		return false;
	}

	// Get the Doc ID of the page pointing to current page
	public String getParentOfOnePage(String child) throws RocksDBException {
		String parent_str = "";
		String allDoc = getAllDocID();
		String[] parents = allDoc.split(" ");
		for (String parent : parents) {
			if (checkIfChildExist(parent, child)) {
				if (parent_str.equals("")) {
					parent_str = parent_str + parent;
				} else {
					parent_str = parent_str + " " + parent;
				}
			}
		}
		return parent_str;
	}

	// Print the Doc ID of the page pointing to current page
	public void printParentOfOnePage(String child) throws RocksDBException {
		String parent_str = "Parent of " + child + ":";
		String allDoc = getAllDocID();
		String[] parents = allDoc.split(" ");
		for (String parent : parents) {
			if (checkIfChildExist(parent, child)) {
				parent_str = parent_str + " " + parent + "(Outgoing Link:" + getNumOfOutgoingLink(parent) + " | PR:"
						+ Math.round(getPageRankFromDB(parent) * 1000) / 1000 + ")";
			}
		}
		System.out.println(parent_str);
	}

	// ========== PageRank ==========

	// Get the number of outgoing link of a page
	public int getNumOfOutgoingLink(String docID) throws RocksDBException {
		byte[] content = parentchilddb.get(docID.getBytes());
		if (content != null) {
			return new String(content).split(" ").length;
		}
		return 0;
	}

	// Get PageRank Array
	public double[] getPageRankArray() throws RocksDBException {
		String docIDList = getAllDocID();
		String[] docs = docIDList.split(" ");
		double[] pr_val = new double[docs.length];
		for (String doc : docs) {
			int id = Integer.valueOf(doc.substring(3, doc.length()));
			pr_val[id] = getPageRankFromDB(doc);
		}
		return pr_val;
	}

	// Print All PageRank in db
	public void printAllPageRank() throws RocksDBException {
		System.out.println(">>> Printing All PageRank...");
		RocksIterator iter = pagerankdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
		}
		iter.close();
	}

	// Delete All PageRank in db
	public void delAllPageRank() throws RocksDBException {
		System.out.println(">>> Deleting All PageRank...");
		RocksIterator iter = pagerankdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			pagerankdb.remove(new String(iter.key()).getBytes());
		}
		iter.close();
	}

	// Print PageRank Array
	public void printPageRankArray() throws RocksDBException {
		double[] pr_val = getPageRankArray();
		for (int i = 0; i < pr_val.length; i++) {
			System.out.println("PR of Doc " + i + ": " + pr_val[i]);
		}
	}

	// Print First N doc in PageRank Array
	public void printFirstNPageRankArray(int n) throws RocksDBException {
		printNtoMPageRankArray(0, n - 1);
	}

	// Print the n-th to m-th doc in PageRak Array
	public void printNtoMPageRankArray(int m, int n) throws RocksDBException {
		double[] pr_val = getPageRankArray();
		for (int i = m; i >= m && i < pr_val.length && i <= n; i++) {
			System.out.println("PR of Doc " + i + ": " + pr_val[i]);
		}
	}

	// Get the sum of the PageRank Array
	public double sumOfPageRankArray(double[] pr_val) throws RocksDBException {
		double result = 0;
		for (double pr : pr_val) {
			result += pr;
		}
		return result;
	}

	// Initilaise PageRank Value
	public void initPageRankValue() throws RocksDBException {
		String docIDList = getAllDocID();
		String[] docs = docIDList.split(" ");
		for (String doc : docs) {
			addPageRankIntoDB(doc, 1.0);
		}
		System.out.println("PageRank Database Initialised");
	}

	// Add PageRank into DB
	public void addPageRankIntoDB(String docID, double pr) throws RocksDBException {
		double precision = roundOff(pr, prPrecision);
		byte[] content = (String.valueOf(precision)).getBytes();
		pagerankdb.put(docID.getBytes(), content);
	}

	// Check Convergence of PageRank Value
	public boolean checkConvergence(double[] old_pr, double[] new_pr) throws RocksDBException {
		double error = 0.0;
		// System.out.println("Checking Convergecne!!!!!");
		for (int i = 0; i < old_pr.length; i++) {
			double thisError = Math.abs(old_pr[i] - new_pr[i]);
			if (thisError < thersold) {
				continue;
			}
			error = error + thisError;
		}
		if (error < thersold) {
			return true;
		}
		return false;
	}

	// Update PageRank from PR Array
	public void updatePageRankIntoDB(double dump_fac, int iter, boolean normalise) throws RocksDBException {
		double[] old_pr_val = getPageRankArray();
		for (int iter_count = 0; iter_count < iter; iter_count++) {
			double[] new_pr_val = calculatePageRankArray(dump_fac);
			double new_pr_sum = sumOfPageRankArray(new_pr_val);
			if (checkConvergence(old_pr_val, new_pr_val)) {
				System.out.println(
						"!!! --- Convergence Deteced! Update Terminated at Iteration " + iter_count + " --- !!!");
				break;
			}
			for (int i = 0; i < new_pr_val.length; i++) {
				String index = "doc" + i;
				if (normalise) {
					addPageRankIntoDB(index, new_pr_val[i] / new_pr_sum);
				} else {
					addPageRankIntoDB(index, new_pr_val[i]);
				}
			}
			old_pr_val = new_pr_val;
		}
	}

	// Get PageRank Value from DB
	public double getPageRankFromDB(String docID) throws RocksDBException {
		byte[] content = pagerankdb.get(docID.getBytes());
		if (content != null) {
			String pr_str = new String(content);
			double pr = Double.valueOf(pr_str).doubleValue();
			return pr;
		}
		return 0;
	}

	// Get the value of PR(T1)/C(T1)
	public double getPRDivideOutgoingLink(String parentID) throws RocksDBException {
		if (Math.abs(getNumOfOutgoingLink(parentID) - 0) < thersold) {
			return 0.0;
		}
		return getPageRankFromDB(parentID) / getNumOfOutgoingLink(parentID);
	}

	// Get the PR value of a page
	public double getPageRankValue(double dump_fac, String docID) throws RocksDBException {
		String parentDocIDList = getParentOfOnePage(docID);
		String[] parentDocs = parentDocIDList.split(" ");
		double content = 0;
		for (String parent : parentDocs) {
			content = content + getPRDivideOutgoingLink(parent);
		}
		return ((1 - dump_fac) + dump_fac * content);
	}

	// Power function for PageRank
	public double power(int numPower) {
		double power = 1.0;
		for (int i = 0; i < numPower; i++) {
			power *= 10;
		}
		return power;
	}

	// Round off function for PageRank
	public double roundOff(double input, int numOfdp) {
		double power = power(numOfdp);
		return Math.round(input * power) / power;
	}

	// Calculate the PageRank of all document (Without step)
	public double[] calculatePageRankArray(double dump_fac) throws RocksDBException {
		String docIDList = getAllDocID();
		int doc_len = docIDList.split(" ").length;
		double[] new_pr = new double[doc_len];
		for (int i = 0; i < doc_len; i++) {
			String target = "doc" + String.valueOf(i);
			new_pr[i] = getPageRankValue(dump_fac, target);
		}
		return new_pr;

	}

	// ========== Other Functions ==========

	// Clear the data from RocksDB
	public void clearAll() throws RocksDBException {
		delAllDocMap();
		delAllWordMap();
		delAllMetadata();
		delAllInvertedIndex();
		delAllForwardIndex();
		delAllParentChild();
		delAllPageRank();
		System.out.println(">>> Database cleared!");
	}

	// Print out all data in the RocksDB
	public void printAll() throws RocksDBException {
		printAllDocMapping();
		printAllWordMapping();
		printAllMetadata();
		printAllInvertedIndex();
		printAllForwardIndex();
		printAllParentChild();
		printAllPageRank();
	}

};
package InvIndex;

import org.rocksdb.RocksDB;

import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;  
import org.rocksdb.RocksIterator;

public class InvertedIndex
{
    private RocksDB docmapdb;
    private RocksDB wordmapdb;
    private RocksDB inverteddb;
    private RocksDB forwarddb;
    private RocksDB metadatadb;
    private RocksDB parentchilddb;
    private RocksDB pagerankdb;
    
    private Options options;
    
    private String db1 = "db/doc";
    private String db2 = "db/word";
    private String db3 = "db/invert";
    private String db4 = "db/forward";
    private String db5 = "db/metadata";
    private String db6 = "db/parentchild";
    private String db7 = "db/pagerank";
    
    private int wordNextID;
    private int docNextID;
    private int prPrecision;
    private double thersold;
    
    // 
    public InvertedIndex() throws RocksDBException
    {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        
        this.wordNextID = 0;
        this.docNextID = 0;
        this.prPrecision = 7;
        this.thersold = 1/power(prPrecision);
              
        // create and open these databases
        this.docmapdb = RocksDB.open(options, db1);
        this.wordmapdb = RocksDB.open(options, db2);
        this.inverteddb = RocksDB.open(options, db3);
        this.forwarddb = RocksDB.open(options, db4);
        this.metadatadb = RocksDB.open(options, db5);
        this.parentchilddb = RocksDB.open(options, db6);
        this.pagerankdb = RocksDB.open(options, db7);
    }
    
 
    
 // ===============================  Document Mapping Part  ===================================
    
    /*
     *  Add new Document Mapping pair (e.g. "https://www.cse.ust.hk/" : "doc0")
     */
    public void addDocMappingEntry(String url) throws RocksDBException{
    	byte[] content = docmapdb.get(url.getBytes());
    	if(content != null){
    		// Stop adding if mapping exists
            return;
        } else {
	        // Add new doc mapping
	        content = ("doc" + this.docNextID).getBytes();
	        this.docNextID++;
        }
    	docmapdb.put(url.getBytes(), content);
    }
    
    /*
     *  Get all existing Document ID (e.g. "doc1 doc5 doc2")
     */
    public String getAllDocID() throws RocksDBException{
    	String docIDList="";
    	RocksIterator iter = docmapdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		docIDList = docIDList + new String(iter.value()) + " ";
    	}
    	return docIDList;
    }
    
    /*
     *  Get the Doc ID by the URL (e.g. "https://www.cse.ust.hk/" returns "doc0")
     */
    public String getDocIDbyURL(String url) throws RocksDBException {
    	String result = "";
    	byte[] content = docmapdb.get(url.getBytes());
    	if(content != null) {
    		// if docID exist
    		result = new String(content);
    	}
    	else {
    		// if docID not exist, make one
    		addDocMappingEntry(url);
    		result = getDocIDbyURL(url);
    	}
    	return result;
    }
    
    /*
     *  Get the URL by the Doc ID (e.g. "doc0" returns "https://www.cse.ust.hk/")
     */
    public String getURLbyDocID(String docID) throws RocksDBException{
    	RocksIterator iter = docmapdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String value = new String(iter.value());
    		if(value.equals(docID)) {
    			return new String(iter.key());
    		}
    	}
    	return "";
    }
    
    /*
     * Delete Document Mapping by URL in RocksDB
     */
    public void delDocMapbyURL(String url) throws RocksDBException
    {
    	docmapdb.remove(url.getBytes());
    }  
    
    /*
     * Delete Document Mapping by URL
     */
    public void delDocMapbyDocID(String docID) throws RocksDBException
    {
    	String url = getURLbyDocID(docID);
    	delDocMapbyURL(url);
    } 
    
    /*
     * Delete All Document Mapping
     */
    public void delAllDocMap() throws RocksDBException
    {
    	System.out.println(">>> Deleting All Word Mapping...");
    	RocksIterator iter = docmapdb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	docmapdb.remove(new String(iter.key()).getBytes());
        }
    } 
    
    /*
     *  Print out All Document Mapping pairs
     */
    public void printAllDocMapping() throws RocksDBException{
    	System.out.println(">>> Printing All Doc Mapping...");
    	RocksIterator iter = docmapdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
// ===============================  Word Mapping Part  ===================================
    
    /*
     * Add new word mapping pair (e.g. "abd": "word1")
     */
    public void addWordMappingEntry(String word) throws RocksDBException{
    	byte[] content = wordmapdb.get(word.getBytes());
    	if(content != null){
    		// Stop adding if mapping exists
            return;
        } else {
        	// Add new word mapping
            content = ("word" + this.wordNextID).getBytes();
            this.wordNextID++;
        }   
    	wordmapdb.put(word.getBytes(), content);
    }
    
    /*
     *  Get all existing Word ID (e.g. "word1 word5 word2")
     */
    public String getAllWordID() throws RocksDBException{
    	RocksIterator iter = wordmapdb.newIterator();
    	String docIDList="";
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		docIDList = docIDList + new String(iter.value()) + " ";
    	}
    	return docIDList;
    }
    
    /*
     *  Get the WordID by the Word (e.g. "abd" => "word1")
     */
    public String getWordIDbyWord(String word) throws RocksDBException {
    	byte[] content = wordmapdb.get(word.getBytes());
    	return new String(content);
    }
    
    /*
     *  Get the Word by the Word ID (e.g. "word1" returns "abd")
     */
    public String getWordbyWordID(String wordID) throws RocksDBException{
    	RocksIterator iter = wordmapdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		if(new String(iter.value()).equals(wordID)) {
    			return new String(iter.key());
    		}
    	}
    	return "";
    }
    
    /*
     * Delete Word Mapping by Word in the Word Mapping
     */
    public void delWordMapbyWord(String word) throws RocksDBException
    {
    	wordmapdb.remove(word.getBytes());
    }  
    
    /*
     * Delete Word Mapping by WordID in the Word Mapping
     */
    public void delWordMapbyWordID(String wordID) throws RocksDBException
    {
    	String word = getWordbyWordID(wordID);
    	wordmapdb.remove(word.getBytes());
    }  
    
    /*
     * Delete All Word Mapping
     */
    public void delAllWordMap() throws RocksDBException
    {
    	System.out.println(">>> Deleting All Word Mapping...");
    	RocksIterator iter = wordmapdb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	wordmapdb.remove(new String(iter.key()).getBytes());
        }
    } 
    
    /*
     *  Print out All Word Mapping pairs
     */
    public void printAllWordMapping() throws RocksDBException{
    	System.out.println(">>> Printing All Word Mapping...");
    	RocksIterator iter = wordmapdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
 // ===============================  Metadata Part  ===================================
    
    /*
     * Add Metadata of the URL into RocksDB ("doc1" : "title:lastmodification:size:lang")
     */
    public void addMetadata(String url, String title, String lm, int size, String lang, int level) throws RocksDBException{
 	   String docID = getDocIDbyURL(url);
 	   // Overwrite the old data if crawl again
 	   byte[] content = (title + ":" + lm + ":" + size + ":" + lang).getBytes();
 	   metadatadb.put(docID.getBytes(), content);
    }
    
    /*
     * Get all metadata of the URL
     */
    public Vector<String> getMetadata(String url) throws RocksDBException{
 	   Vector<String> result = new Vector<String>();
 	   String docID = getDocIDbyURL(url);
 	   byte[] content = metadatadb.get(docID.getBytes());
 	   if(content != null) {
 		   String data = new String(content);
 		   for (String d:data.split(":")) {
 			   result.add(d);
 		   }
 	   }
 	   return result;
    }
    
    /*
     *  Get Title of the URL
     */
    public String getTitlebyURL(String url) throws RocksDBException{
    	Vector<String> meta = getMetadata(url);
    	if(meta.size()!=0) {
    		return meta.get(0);
    	}
    	return "";
    }
    
    /*
     *  Get Last Modification Date of the URL
     */
    public String getLastModifiedbyURL(String url) throws RocksDBException{
    	Vector<String> meta = getMetadata(url);
    	if(meta.size()!=0) {
    		return meta.get(1);
    	}
    	return "";
    }
    
    /*
     *  Get Size of the URL
     */
    public String getSizebyURL(String url) throws RocksDBException{
    	Vector<String> meta = getMetadata(url);
    	if(meta.size()!=0) {
    		return meta.get(2);
    	}
    	return "";
    }
    
    /*
     *  Get Lang of the URL
     */
    public String getLangbyURL(String url) throws RocksDBException{
    	Vector<String> meta = getMetadata(url);
    	if(meta.size()!=0) {
    		return meta.get(3);
    	}
    	return "";
    }
    
    /*
     * Delete All Metadata
     */
    public void delAllMetadata() throws RocksDBException
    {
    	System.out.println(">>> Deleting All Metadata...");
    	RocksIterator iter = metadatadb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	metadatadb.remove(new String(iter.key()).getBytes());
        }
    } 
    
    /*
     *  Print out All Metadata
     */
    public void printAllMetadata() throws RocksDBException{
    	System.out.println(">>> Printing All Metadata...");
    	RocksIterator iter = metadatadb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
    /*
     *  Print out All Metadata by Doc ID
     */
    public void printAllMetadataByDocID(String docID) throws RocksDBException{
    	System.out.println(">>> Printing out Metadata by Doc ID...");
    	byte[] content = metadatadb.get(docID.getBytes());
    	if(content != null) {
    		System.out.println(new String(content));
    	}
    }
    
 // ===============================  Inverted Index Part  =================================== 
    
    /*
     * Add word into inverted index (e.g. "word1" : "doc0:2 doc2:5"
     */
    public void addInvertedIndex(String url, String word, int freq) throws RocksDBException {
    	String docID = getDocIDbyURL(url);
    	String wordID = getWordIDbyWord(word);
    	byte[] content = inverteddb.get(wordID.getBytes());
    	if(content != null) {
    		boolean updated = false;
    		String update_freq = "";
    		String[] docFreq_list = new String(content).split(" ");
    		for (String docFreqPair:docFreq_list) {
    			if(!docFreqPair.contains(":")) {
    				continue;
    			}
    			String[] pair = docFreqPair.split(":");
    			String d = pair[0];
    			String f = pair[1];
    			// Check if the record exists
    			if(d.equals(docID)) {
    				// Need Update
    				update_freq = update_freq + docID + ":" + String.valueOf(freq) + " ";
    				updated = true;
    			}else {
    				// No need update
    				update_freq = update_freq + d + ":" + f + " ";
    			}
    		}
			// If have not update, append it
			if(!updated) {
				update_freq = update_freq + docID + ":" + String.valueOf(freq) + " ";
			}
    		content = (update_freq).getBytes();
    	} else {
    		// Add the new inverted indexing for the wordID
    		content = (docID + ":" + freq).getBytes();
    	}
    	inverteddb.put(wordID.getBytes(), content);
    }
    
    /*
     * Delete All Inverted Index
     */
    public void delAllInvertedIndex() throws RocksDBException
    {
    	System.out.println(">>> Deleting All Inverted Index...");
    	RocksIterator iter = inverteddb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	inverteddb.remove(new String(iter.key()).getBytes());
        }
    } 
    
    /*
     *  Print out All Inverted Index
     */
    public void printAllInvertedIndex() throws RocksDBException{
    	System.out.println(">>> Printing All Inverted Index...");
    	RocksIterator iter = inverteddb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
    /*
     *  Print out All Inverted Index by Word ID
     */
    public void printAllInvertedIndex(String wordID) throws RocksDBException{
    	System.out.println(">>> Printing out Inverted Index by Word ID...");
    	byte[] content = inverteddb.get(wordID.getBytes());
    	if(content != null) {
    		System.out.println(new String(content));
    	}
    }
    
    
 // ===============================  Forward Index Part  ====================================
    
    /*
     * Add data into forward index (e.g. "doc1" : "word4:2 word5:1")
     */
    public void addForwardIndex(String url, String word, int count) throws RocksDBException{
    	String docID = getDocIDbyURL(url);
    	byte[] content = forwarddb.get(docID.getBytes());
    	if(content != null){
    		boolean updated = false;
    		String update_freq = "";
    		String[] wordFreq_list = new String(content).split(" ");
    		for (String wordFreqPair:wordFreq_list) {
    			String[] pair = wordFreqPair.split(":");
    			String w = pair[0];
    			String f = pair[1];
    			// Check if the record exists
    			if(w.equals(word)) {
    				// Need Update
    				update_freq = update_freq + word + ":" + String.valueOf(count) + " ";
    				updated = true;
    			}else {
    				// No need update
    				update_freq = update_freq + w + ":" + f + " ";
    			}  			
    		}
    		// If have not update, append it
			if(!updated) {
				update_freq = update_freq + word + ":" + String.valueOf(count) + " ";
			}  
            content = (update_freq).getBytes();
        } else {
            // Add new forward indexing for the docID
            content = (word + ":" + String.valueOf(count)).getBytes();
        }   
    	forwarddb.put(docID.getBytes(), content);
    }
    
    /*
     * Get data in forward index by URL
     */
    public String getForwardIndex(String url)throws RocksDBException{
    	String docID = getDocIDbyURL(url);
    	byte[] content = forwarddb.get(docID.getBytes());
    	String result = "";
    	if(content != null){
    		String wordFreq = new String(content);
    		String[] wordFreqPair = wordFreq.split(" ");
    		for(String pair: wordFreqPair) {
    			if(!pair.contains(":")) {
    				continue;
    			}
    			String keyword = pair.split(":")[0];
    			String freq = pair.split(":")[1];
    			result = result + keyword + " " + freq + "; ";
    		}
        }
        return result + "\n";
    }

    /*
     * Delete All Forward Index
     */
    public void delAllForwardIndex() throws RocksDBException
    {
    	System.out.println(">>> Deleting All Forward Index...");
    	RocksIterator iter = forwarddb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	forwarddb.remove(new String(iter.key()).getBytes());
        }
    } 
   
    /*
     *  Print out All Forward Index
     */
    public void printAllForwardIndex() throws RocksDBException{
    	System.out.println(">>> Printing All Forward Index...");
    	RocksIterator iter = forwarddb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
    /*
     *  Print out All Forward Index by Doc ID
     */
    public void printAllForwardIndexbyDocID(String docID) throws RocksDBException{
    	System.out.println(">>> Printing out Forward Index by Doc ID...");
    	byte[] content = forwarddb.get(docID.getBytes());
    	if(content != null) {
    		System.out.println(new String(content));
    	}
    }
   
 // ========================  Linkage Part (Parent-Child Relation)  =========================
    
    /*
     * Add the link between the parent and child relation
     */
    public void addPCRelation(String p_url, String c_url) throws RocksDBException{
    	String parentID = getDocIDbyURL(p_url);
    	String childID = getDocIDbyURL(c_url);
    	byte[] content = parentchilddb.get(parentID.getBytes());
    	if(content != null){
    		String old_children = new String(content);
    		if (!old_children.contains(childID)) {
    			content = (new String(content) + " " + childID).getBytes();
    		}
            
        } else {
            content = (childID).getBytes();
        }   
    	parentchilddb.put(parentID.getBytes(), content);
    }
    
    /*
     * Get the parent child relation by the parent URL
     */
    public String getPCRelation(String p_url) throws RocksDBException{
    	String result = new String();
    	String parentID = getDocIDbyURL(p_url);
    	byte[] content = parentchilddb.get(parentID.getBytes());
    	if(content != null){
    		String new_content = new String(content);
    		String[] children = new_content.split(" ");
    		int count = 0;
    		for (String child: children){
    			count++;
    			result = result + "Child Link " + String.valueOf(count) + ": " + getURLbyDocID(child) + "\n";
    		}
    		return result;
    	}else {
    		return "";
    	}
    }

    /*
     *  Print out All Parent Child Relation
     */
    public void printAllParentChild() throws RocksDBException{
    	System.out.println(">>> Printing All Parent Child Relation...");
    	RocksIterator iter = parentchilddb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
    /*
     *  Print out All Parent Child Relation
     */
    public void delAllParentChild() throws RocksDBException{
    	System.out.println(">>> Deleting All Parent Child Relation...");
    	RocksIterator iter = parentchilddb.newIterator();
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
        	parentchilddb.remove(new String(iter.key()).getBytes());
        }
    }
    
    /*
     * Check if the doc ID has the child doc ID
     */
    public boolean checkIfChildExist(String p_doc, String c_doc) throws RocksDBException{
    	byte[] content = parentchilddb.get(p_doc.getBytes());
    	if(content != null){
    		String children = new String(content);
    		if (children.contains(c_doc)) {
    			return true;
    		}
    	}
    	return false;
    }   
    
    
    /*
     * Get the Doc ID of the page pointing to current page
     */
    public String getParentOfOnePage(String child) throws RocksDBException{
    	String parent_str = "";
    	String allDoc = getAllDocID();
		String[] parents = allDoc.split(" ");
    	for(String parent:parents) {
	    	if(checkIfChildExist(parent,child)) {
	    		if(parent_str.equals("")) {
	    			parent_str = parent_str + parent;
	    		}else {
	    			parent_str = parent_str + " " + parent;
	    		}
	    	}
    	}
    	return parent_str;
    }
      
    /*
     * Print the Doc ID of the page pointing to current page
     */
    public void printParentOfOnePage(String child) throws RocksDBException{
    	String parent_str = "Parent of " + child + ":";
    	String allDoc = getAllDocID();
		String[] parents = allDoc.split(" ");
    	for(String parent:parents) {
	    	if(checkIfChildExist(parent,child)) {
	    		parent_str = parent_str + " " + parent+ "(Outgoing Link:" + getNumOfOutgoingLink(parent) + " | PR:" + Math.round(getPageRankFromDB(parent)*1000)/1000 + ")";
	    	}
    	}
    	System.out.println(parent_str);
    }
    
 // =============================  PageRank  ===========================
    
    /*
     * Get the number of outgoing link of a page
     */
    public int getNumOfOutgoingLink(String docID) throws RocksDBException{
    	byte[] content = parentchilddb.get(docID.getBytes());
    	if(content != null){
    		return new String(content).split(" ").length;
    	}
    	return 0;
    }
    
    /*
     * Get PageRank Array
     */
    public double[] getPageRankArray()throws RocksDBException{
    	String docIDList = getAllDocID();
		String[] docs = docIDList.split(" ");
    	double[] pr_val = new double[docs.length];
		for(String doc:docs) {
			int id = Integer.valueOf(doc.substring(3,doc.length()));
			pr_val[id] = getPageRankFromDB(doc);
		}
		return pr_val;
    }
    
    /*
     * Print All PageRank in db
     */
    public void printAllPageRank() throws RocksDBException{
    	System.out.println(">>> Printing All PageRank...");
    	RocksIterator iter = pagerankdb.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
    	}
    }
    
    /*
     * Delete All PageRank in db
     */
    public void delAllPageRank() throws RocksDBException{
    	System.out.println(">>> Deleting All PageRank...");
    	RocksIterator iter = pagerankdb.newIterator();
    	for(iter.seekToFirst(); iter.isValid(); iter.next()){
    		pagerankdb.remove(new String(iter.key()).getBytes());
        }
    }
    
    /*
     * Print PageRank Array
     */
    public void printPageRankArray()throws RocksDBException{
    	double[] pr_val = getPageRankArray();
		for(int i = 0 ; i < pr_val.length; i++) {
			System.out.println("PR of Doc " + i + ": " + pr_val[i]);
		}
    }
    
    /*
     * Print First N doc in PageRank Array
     */
    public void printFirstNPageRankArray(int n)throws RocksDBException{
    	printNtoMPageRankArray(0,n-1);
    }
    
    /*
     * Print the n-th to m-th doc in PageRank Array
     */
    public void printNtoMPageRankArray(int m, int n)throws RocksDBException{
    	double[] pr_val = getPageRankArray();
		for(int i = m ; i >= m && i < pr_val.length && i <= n; i++) {
			System.out.println("PR of Doc " + i + ": " + pr_val[i]);
		}
    }
    
    /*
     * Get the sum of the PageRank Array
     */
    public double sumOfPageRankArray(double[] pr_val) throws RocksDBException{
    	double result = 0;
    	for(double pr:pr_val) {
			result += pr;
		}
    	return result;
    }
        
    /*
     * Initilaise PageRank Value
     */
    public void initPageRankValue() throws RocksDBException{
    	String docIDList = getAllDocID();
		String[] docs = docIDList.split(" ");
		for(String doc:docs) {
			addPageRankIntoDB(doc,1.0);
		}
		System.out.println("PageRank Database Initialised");
    }
    
    /*
     * Add PageRank into DB
     */
    public void addPageRankIntoDB(String docID, double pr) throws RocksDBException{
    	double precision = roundOff(pr,prPrecision);
    	byte[] content = (String.valueOf(precision)).getBytes();
    	pagerankdb.put(docID.getBytes(), content);
    }
    
    /*
     * Check Convergence of PageRank Value
     */
    public boolean checkConvergence(double[] old_pr, double[] new_pr) throws RocksDBException {
    	double error = 0.0;
//    	System.out.println("Checking Convergecne!!!!!");
    	for(int i = 0; i < old_pr.length; i++) {
    		double thisError = Math.abs(old_pr[i]-new_pr[i]);
//    		System.out.println(old_pr[i] + " => " + new_pr[i] + " \t| Error: " + Math.abs(old_pr[i]-new_pr[i]));
    		if(thisError < thersold) {
    			continue;
    		}
    		error = error + thisError;
    	}
    	if(error<thersold){    	
    		return true;
    	}
    	return false;
    }
    
    /*
     * Update PageRank from PR Array
     */
    public void updatePageRankIntoDB(double dump_fac,int iter, boolean normalise) throws RocksDBException{
    	double[] old_pr_val = getPageRankArray();
    	for(int iter_count = 0 ; iter_count < iter ; iter_count++) {
	    	double[] new_pr_val = calculatePageRankArray(dump_fac);
	    	double new_pr_sum = sumOfPageRankArray(new_pr_val);
	    	if (checkConvergence(old_pr_val,new_pr_val)) {
	    		System.out.println("!!! --- Convergence Deteced! Update Terminated at Iteration " + iter_count + " --- !!!");
	    		break;
	    	}
//	    	System.out.println(">>> Start Updating PageRank for Iteration" + (iter_count+1) + " <<<");
	    	for(int i = 0; i < new_pr_val.length;i++) {
	    		String index = "doc" + i;
	    		if(normalise) {
	    			addPageRankIntoDB(index,new_pr_val[i]/new_pr_sum);
	    		}else {
	    			addPageRankIntoDB(index,new_pr_val[i]);
	    		}
	    	}
//	    	System.out.println(">>> Finished Updating PageRank for Iteration" + (iter_count+1) + " <<<\n");
	    	old_pr_val = new_pr_val;
    	}
    }
    
    /*
     * Get PageRank Value from DB
     */
    public double getPageRankFromDB(String docID) throws RocksDBException{
    	byte[] content = pagerankdb.get(docID.getBytes());
    	if(content != null){
    		String pr_str = new String(content);
    		double pr = Double.valueOf(pr_str).doubleValue();
    		return pr;
    	}
    	return 0;
    }
    
    /*
     * Get the value of PR(T1)/C(T1)
     */
    public double getPRDivideOutgoingLink(String parentID) throws RocksDBException{
    	if(Math.abs(getNumOfOutgoingLink(parentID)-0)<thersold) {
    		return 0.0;
    	}
    	return getPageRankFromDB(parentID)/getNumOfOutgoingLink(parentID);
    }
    
    /*
     * Get the PR value of a page
     */
    public double getPageRankValue(double dump_fac, String docID) throws RocksDBException{
    	String parentDocIDList = getParentOfOnePage(docID);
    	String[] parentDocs = parentDocIDList.split(" ");
		double content = 0;
		for(String parent:parentDocs) {
			content = content + getPRDivideOutgoingLink(parent);
		}
    	return ((1-dump_fac)+dump_fac*content);
    }
    
    /*
     * Calculate the PageRank of all document (Without Steps)
     */
//    public void calculatePageRankInDetails(double dump_fac) throws RocksDBException{
//    	String docIDList = getAllDocID();
//		int doc_len = docIDList.split(" ").length;
//		double[] new_pr = new double[doc_len];
//		for(int i = 0;i < doc_len;i++) {
//	    	String target = "doc"+String.valueOf(i);
//			System.out.println("******************************************************");
//	    	System.out.println("Parent of " + target + ": " + getParentOfOnePage(target));
//	    	String parentDocIDList = getParentOfOnePage(target);
//			String[] parentDocs = parentDocIDList.split(" ");
//			int count = 1;
//			double content= 0;
//			System.out.println("Dumping Factor: " + dump_fac);
//			for(String parent:parentDocs) {
//				System.out.println("-------------------------------------------");
//				System.out.println("Parent " + count + ": " + parent);
//				System.out.println("PR of " + parent + " = " + getPageRankFromDB(parent));
//				System.out.println("No. of outgoing link off " + parent + " = " + getNumOfOutgoingLink(parent));
//				System.out.println("item " + count + ": " + getPRDivideOutgoingLink(parent));
//				content = content + getPRDivideOutgoingLink(parent);
//				count++;
//			}
//			System.out.println("-------------------------------------------");
//			System.out.println("Overall PR of " + target + " = " + getPageRankValue(dump_fac,target));
//			new_pr[i]=roundOff(getPageRankValue(dump_fac,target),prPrecision);
//		}
//		
//    }

    /*
     * Power function for PageRank
     */
    public double power(int numPower) {
    	double power = 1.0;
    	for(int i = 0; i < numPower; i ++) {
    		power *= 10;
    	}
    	return power;
    }
    
    /*
     * Round off function for PageRank
     */
    public double roundOff(double input,int numOfdp) {
    	double power = power(numOfdp);
    	return Math.round(input*power)/power;
    }
    
    /*
     * Calculate the PageRank of all document (Without step)
     */
    public double[] calculatePageRankArray(double dump_fac) throws RocksDBException{
    	String docIDList = getAllDocID();
		int doc_len = docIDList.split(" ").length;
		double[] new_pr = new double[doc_len];
		for(int i = 0;i < doc_len;i++) {
	    	String target = "doc"+String.valueOf(i);
			new_pr[i]=getPageRankValue(dump_fac,target);
		}
		return new_pr;
		
    }
        
 // =========================== Other Functions ================================================
    
     
   /*
    * Clear the data from RocksDB
    */
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
   
   /*
    * Print out all data in the RocksDB
    */
   public void printAll() throws RocksDBException {
	   printAllDocMapping();
	   printAllWordMapping();
	   printAllMetadata();
	   printAllInvertedIndex();
	   printAllForwardIndex();
	   printAllParentChild();
	   printAllPageRank();
   }   
   
     
}
;
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
    private RocksDB db;
    private Options options;
    private int wordNextID;
    private int docNextID;
    private int prPrecision;
    private double thersold;
    
    // 
    public InvertedIndex(String dbPath) throws RocksDBException
    {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.wordNextID = 0;
        this.docNextID = 0;
        this.prPrecision = 7;
        this.thersold = 1/power(prPrecision);

        // create and open the database
        this.db = RocksDB.open(options, dbPath);
    }
    
 // ===============================  Document Mapping Part  ===================================
    
    /*
     *  Add Document Mapping
     */
    public void addDocMappingEntry(String url) throws RocksDBException{
    	byte[] content = db.get(url.getBytes());
    	
    	if(content != null){
            return;
        } else {
	        //create new key value pair
	        content = ("doc" + this.docNextID).getBytes();
	        this.docNextID++;
        }
        db.put(url.getBytes(), content);
    }
    
    public String getAllDocID() throws RocksDBException{
    	RocksIterator iter = db.newIterator();
    	String docIDList="";
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String key = new String(iter.key());
    		if(key.contains("docMapping_")) {
    			docIDList = docIDList + new String(iter.value()) + " ";
    		}
    	}
    	return docIDList;
    }
    
    /*
     *  Get the Doc ID by the URL (e.g. "https://www.cse.ust.hk/" returns "doc0")
     */
    public String getDocIDbyURL(String url) throws RocksDBException {
    	String result = "";
    	String new_url = "docMapping_" + url;
    	byte[] content = db.get(new_url.getBytes());
    	if(content != null) {
    		result = new String(content);
    	}
    	return result;
    }
    
    /*
     *  Get the URL by the Doc ID (e.g. "doc0" returns "https://www.cse.ust.hk/")
     */
    public String getURLbyDocID(String docID) throws RocksDBException{
    	RocksIterator iter = db.newIterator();
    	String url = "";
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String value = new String(iter.value());
    		if(value.equals(docID)) {
    			url = new String(iter.key());
    			url = url.replace("docMapping_","");
    			break;
    		}
    	}
    	return url;
    }
    
// ===============================  Word Mapping Part  ===================================
    
    /*
     * Add Word Mapping
     */
    public void addWordMappingEntry(String word) throws RocksDBException{
    	byte[] content = db.get(word.getBytes());
  
    	if(content != null){
            return;
        } else {
            //create new key value pair
            content = ("word" + this.wordNextID).getBytes();
            this.wordNextID++;
        }   
        db.put(word.getBytes(), content);
    }
    
    /*
     *  Get the wordID by the word (e.g. "abd" => "word1")
     */
    public String getWordID(String word) throws RocksDBException {
    	String new_word = "wordMapping_" + word;
    	byte[] content = db.get(new_word.getBytes());
    	return new String(content);
    }
    
    /*
     * Delete data in RocksDB
     */
    public void delEntry(String word) throws RocksDBException
    {
        // Delete the word and its list from the hashtable
        // ADD YOUR CODES HERE
        db.remove(word.getBytes());
    }  
    
 // ===============================  Metadata Part  ===================================
    
    /*
     * Add Metadata of the URL into RocksDB
     */
    public void metadata(String url, String title, String lm, int size, String lang, int level) throws RocksDBException{
 	   String str = "metadata_" + getDocIDbyURL(url);
 	   byte[] content = db.get(str.getBytes());
 	   if(content != null) {
    		//append
    		content = (new String(content) + " " + title + ":" + lm + ":" + size + ":" + lang).getBytes();
    	} else {
    		//create new inverted_wordID -> docID freq
    		content = (title + ":" + lm + ":" + size + ":" + lang).getBytes();
    	}
    	db.put(str.getBytes(), content);
    }
    
    /*
     * Get all metadata of the URL
     */
    public Vector<String> getMetadata(String url) throws RocksDBException{
 	   Vector<String> result = new Vector<String>();
 	   String str = "metadata_" + getDocIDbyURL(url);
 	   byte[] content = db.get(str.getBytes());
 	   if(content != null) {
 		   String data = new String(content);
 		   String[] metadata =data.split(":");
 		   for (String d:metadata) {
 			   result.add(d);
 		   }
 	   }
 	   
 	   return result;
    }
    
    /*
     *  Get last modified date of the URL
     */
    public String getLastModified(String url) throws RocksDBException{
    	String str = "metadata_" + getDocIDbyURL(url);
    	byte[] content = db.get(str.getBytes());
    	String contentStr = content.toString();
    	System.out.println("contentStr" + contentStr);
    	String[] parts = contentStr.split(":");
    	return parts[0];
    }
    
 // ===============================  Inverted Index Part  =================================== 
    
    /*
     * Add data into inverted index
     */
    public void invert(String url, String word, int freq) throws RocksDBException {
    	String docID = getDocIDbyURL(url);
    	String wordID = getWordID(word);
    	String str = "inverted_" + wordID;
    	
    	byte[] content = db.get(str.getBytes());
    	if(content != null) {
    		//append
    		content = (new String(content) + " " + docID + ":" + freq).getBytes();
    	} else {
    		//create new inverted_wordID -> docID freq
    		content = (docID + ":" + freq).getBytes();
    	}
    	db.put(str.getBytes(), content);
    }
    
 // ===============================  Forward Index Part  ====================================
    
    /*
     * Add data into forward index
     */
    public void forward(String url, String word, int count) throws RocksDBException{
    	String str = getDocIDbyURL(url);
    	str = "forward_" + str;
    	byte[] content = db.get(str.getBytes());
    	if(content != null){
            //append
            content = (new String(content) + " " + word + ":" +String.valueOf(count)).getBytes();
        } else {
            //create new key value pair
            content = (word + ":" + String.valueOf(count)).getBytes();
        }   
        db.put(str.getBytes(), content);
    }
    
    /*
     * Get data in forward index by URL
     */
    public String getForward(String url)throws RocksDBException{
    	String str = getDocIDbyURL(url);
    	str = "forward_" + str;
    	byte[] content = db.get(str.getBytes());
    	String result = "";
    	if(content != null){
            //append
    		String new_content = new String(content);
    		String[] keywordPair = new_content.split(" ");
    		for(String pair: keywordPair) {
    			String keyword = pair.split(":")[0];
    			String freq = pair.split(":")[1];
    			result = result + keyword + " " + freq + "; ";
    		}
        }
        return result + "\n";
    }
   
 // ========================  Linkage Part (Parent-Child Relation)  =========================
    
    /*
     * Add the link between the parent and child relation
     */
    public void addPCRelation(String p_url, String c_url) throws RocksDBException{
    	String parentID = "PCR_" + getDocIDbyURL(p_url);
    	String childID = getDocIDbyURL(c_url);
    	byte[] content = db.get(parentID.getBytes());
    	if(content != null){
            //append
    		String old_child = new String(content);
    		if (!old_child.contains(childID)) {
    			content = (new String(content) + " " + childID).getBytes();
    		}
            
        } else {
            //create new key value pair
            content = (childID).getBytes();
        }   
        db.put(parentID.getBytes(), content);
    }
    
    /*
     * Get the parent child relation by the parent URL
     */
    public String getPCRelation(String p_url) throws RocksDBException{
    	String result = new String();
    	String parentID = "PCR_" + getDocIDbyURL(p_url);
    	byte[] content = db.get(parentID.getBytes());
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
     * Check if the doc ID has the child doc ID
     */
    public boolean checkIfChildExist(String p_doc, String c_doc) throws RocksDBException{
    	String parentID = "PCR_" + p_doc;
    	byte[] content = db.get(parentID.getBytes());
    	if(content != null){
    		String new_content = new String(content);
    		String[] children = new_content.split(" ");
    		for (String child: children) {  
    			if (child.equals(c_doc)) {
    				return true;
    			}
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
     * Get the parent pages of all doc in db
     */
    public String getParentOfEachPage() throws RocksDBException{
    	String result = "";
    	String allDoc = getAllDocID();
		String[] docs = allDoc.split(" ");
    	for(String doc:docs) {
    		result = result + getParentOfOnePage(doc) + "\n";
    	}
    	return result;
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
    
    /*
     * Print the parent pages of all doc in db
     */
    public void printParentOfEachPage() throws RocksDBException{
    	String allDoc = getAllDocID();
		String[] docs = allDoc.split(" ");
    	for(String doc:docs) {
    		printParentOfOnePage(doc);
    	}
    }
    
    /*
     * Get the number of outgoing link of a page
     */
    public int getNumOfOutgoingLink(String docID) throws RocksDBException{
    	String parentID = "PCR_" + docID;
    	byte[] content = db.get(parentID.getBytes());
    	if(content != null){
    		String new_content = new String(content);
    		String[] children = new_content.split(" ");
    		return children.length;
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
//			addPageRankIntoDB(doc,1.0/docs.length);
			addPageRankIntoDB(doc,1.0);
		}
		System.out.println("PageRank Database Initialised");
    }
    
    /*
     * Add PageRank into DB
     */
    public void addPageRankIntoDB(String docID, double pr) throws RocksDBException{
    	double precision = roundOff(pr,prPrecision);
    	String index = "PR_" + docID;
    	byte[] content = (String.valueOf(precision)).getBytes();
        db.put(index.getBytes(), content);
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
	    	System.out.println(">>> Start Updating PageRank for Iteration" + (iter_count+1) + " <<<");
	    	for(int i = 0; i < new_pr_val.length;i++) {
	    		String index = "doc" + i;
	    		if(normalise) {
	    			addPageRankIntoDB(index,new_pr_val[i]/new_pr_sum);
	    		}else {
	    			addPageRankIntoDB(index,new_pr_val[i]);
	    		}
	    	}
//	    	printPageRankArray();
	    	printFirstNPageRankArray(50);
//	    	printNtoMPageRankArray(20,30);
	    	System.out.println(">>> Finished Updating PageRank for Iteration" + (iter_count+1) + " <<<\n");
	    	old_pr_val = new_pr_val;
    	}
    }
    
    /*
     * Get PageRank Value from DB
     */
    public double getPageRankFromDB(String docID) throws RocksDBException{
    	String index = "PR_" + docID;
    	byte[] content = db.get(index.getBytes());
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
    public void calculatePageRankInDetails(double dump_fac) throws RocksDBException{
    	String docIDList = getAllDocID();
		int doc_len = docIDList.split(" ").length;
		double[] new_pr = new double[doc_len];
		for(int i = 0;i < doc_len;i++) {
	    	String target = "doc"+String.valueOf(i);
			System.out.println("******************************************************");
	    	System.out.println("Parent of " + target + ": " + getParentOfOnePage(target));
	    	String parentDocIDList = getParentOfOnePage(target);
			String[] parentDocs = parentDocIDList.split(" ");
			int count = 1;
			double content= 0;
			System.out.println("Dumping Factor: " + dump_fac);
			for(String parent:parentDocs) {
				System.out.println("-------------------------------------------");
				System.out.println("Parent " + count + ": " + parent);
				System.out.println("PR of " + parent + " = " + getPageRankFromDB(parent));
				System.out.println("No. of outgoing link off " + parent + " = " + getNumOfOutgoingLink(parent));
//				System.out.println("item " + count + ": " + getPRDivideOutgoingLink(parent));
				content = content + getPRDivideOutgoingLink(parent);
				count++;
			}
			System.out.println("-------------------------------------------");
			System.out.println("Overall PR of " + target + " = " + getPageRankValue(dump_fac,target));
			new_pr[i]=roundOff(getPageRankValue(dump_fac,target),prPrecision);
		}
		
    }

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
    
    public Vector<String> getURLList()throws RocksDBException {
    	Vector<String> urlsList = new Vector<String>();
    	RocksIterator iter = db.newIterator();
    	for (iter.seekToFirst(); iter.isValid(); iter.next()){
    		String parent = new String(iter.key());
    		
    		// Check only Parent-Child Relationship
    		if(parent.contains("PCR_")) {
    			parent = parent.replace("PCR_","");
    			// Get back the url by the docID
    			parent = getURLbyDocID(parent);
    			// Add parent to URL List if no exist
    			if(!urlsList.contains(parent)) {
    				urlsList.add(parent);
    			}
//    			// Add child to URL List if no exist
//    			String value = new String(iter.value());
//    			String[] children = value.split(" ");
//    			for (String child:children) {
//    				if(!urls.contains(child)) {
//        				urls.add(child);
//        			}
//    			}
    		}   	
    	}
    	return urlsList;
    }
    
   /*
    * Clear the data from RocksDB
    */
   public void clear() throws RocksDBException {
   	RocksIterator iter = db.newIterator();
       for(iter.seekToFirst(); iter.isValid(); iter.next()){
           db.remove(new String(iter.key()).getBytes());
       }
   }
   
   /*
    * Print out all data in the RocksDB
    */
   public void printAll() throws RocksDBException {
   	// Print all the data in the hashtable
       // ADD YOUR CODES HERE
       RocksIterator iter = db.newIterator();
       for(iter.seekToFirst(); iter.isValid(); iter.next()){
           System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
       }
   }   
   
   /*
    * Print out data in the RocksDB
    */
   public void printData(String prefix) throws RocksDBException {
   	// Print all the data in the hashtable
       // ADD YOUR CODES HERE
       RocksIterator iter = db.newIterator();
       for(iter.seekToFirst(); iter.isValid(); iter.next()){
    	   if (new String(iter.key()).contains(prefix)) {
    		   System.out.println(new String(iter.key()) + " = " + new String(iter.value()));
           }
       }
   }
   
// =========================== Function for Web Query =============================
   
   public String query(String input) throws RocksDBException{
	   String res = "";
	   res = input.split(" ")[0];
	   return res;
   }
   
// =========================== Main Program ================================================   
  
    public static void main(String[] args)
    {
        try
        {
            // a static method that loads the RocksDB C++ library.
            RocksDB.loadLibrary();
            
            // modify the path to your database
            String path = "/db";

            InvertedIndex index = new InvertedIndex(path);
            index.clear();
        }
        catch(RocksDBException e)
        {
            System.err.println(e.toString());
        }
    }
}
;
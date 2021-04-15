
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
    
    // 
    InvertedIndex(String dbPath) throws RocksDBException
    {
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        this.options = new Options();
        this.options.setCreateIfMissing(true);
        this.wordNextID = 0;
        this.docNextID = 0;

        // creat and open the database
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
    public String getParent(String child) throws RocksDBException{
    	String parent_str = "Parent of " + child + ":";
    	String allDoc = getAllDocID();
		String[] parents = allDoc.split(" ");
    	for(String parent:parents) {
	    	if(checkIfChildExist(parent,child)) {
	    		parent_str = parent_str + " " + parent+ "(" + getNumOfOutgoingLink(parent) + ")";
	    	}
    	}
    	return parent_str + "\n";
    }
    
    /*
     * Get the parent pages of all doc in db
     */
    public String getParentOfEachPage() throws RocksDBException{
    	String result = "";
    	String allDoc = getAllDocID();
		String[] docs = allDoc.split(" ");
    	for(String doc:docs) {
    		result = result + getParent(doc);
    	}
    	return result;
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

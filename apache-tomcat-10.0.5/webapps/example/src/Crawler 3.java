package mypackage;

import mypackage.StopStem;
import mypackage.InvertedIndex;

import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.jsoup.HttpStatusException;
import java.lang.RuntimeException;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/*
 * The data structure for the crawling URLqueue.
 */
class Link {
	String url;
	int level;

	Link(String url, int level) {
		this.url = url;
		this.level = level;
	}
}

@SuppressWarnings("serial")
/*
 * This is customized exception for those pages that have been visited before.
 */
class RevisitException extends RuntimeException {
	public RevisitException() {
		super();
	}
}

public class Crawler {
	private HashSet<String> urls; // the set of urls that have been visited before
	public Vector<Link> URLqueue; // the queue of URLs to be crawled
	// private int max_crawl_depth = 1; // feel free to change the depth limit of
	// the spider.
	private String domain;
	private String first_url;

	Crawler(String _url) {
		this.URLqueue = new Vector<Link>();
		this.URLqueue.add(new Link(_url, 1));
		this.urls = new HashSet<String>();
		this.first_url = _url;
		this.domain = "cse.ust.hk";
	}

	/*
	 * Send an HTTP request and analyze the response.
	 * 
	 * @return {Response} res
	 * 
	 * @throws HttpStatusException for non-existing pages
	 * 
	 * @throws IOException
	 * 
	 * @throws RocksDBException
	 */
	public Response getResponse(Link focus, InvertedIndex index) throws HttpStatusException, IOException {
		if (this.urls.contains(focus.url)) {
			throw new RevisitException(); // if the page has been visited, break the function
		}

		// Connection conn = Jsoup.connect(url).followRedirects(false);
		// the default body size is 2Mb, to attain unlimited page, use the following.
		Connection conn = Jsoup.connect(focus.url).maxBodySize(0).followRedirects(false).ignoreHttpErrors(true);
		Response res;

		try {
			/* establish the connection and retrieve the response */
			res = conn.execute();
			/* if the link redirects to other place... */
			if (res.hasHeader("location")) {
				String actual_url = res.header("location");
				if (this.urls.contains(actual_url)) {
					throw new RevisitException();
				} else {
					this.urls.add(actual_url);
				}
			} else {
				this.urls.add(focus.url);
			}
		} catch (HttpStatusException e) {
			throw e;
		}

		/* Get the metadata from the result */
		String lastModified = res.header("last-modified");
		int size = res.bodyAsBytes().length;
		String title = res.parse().title();
		String htmlLang = res.parse().select("html").first().attr("lang");
		String bodyLang = res.parse().select("body").first().attr("lang");
		String lang = htmlLang + bodyLang;

		try {
			index.addMetadata(focus.url, title, lastModified, size, lang, focus.level);
		} catch (RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

	/*
	 * Extract words in the web page content. note: use StringTokenizer to tokenize
	 * the result
	 * 
	 * @param {Document} doc
	 * 
	 * @return {Vector<String>} a list of words in the web page body
	 */
	public Vector<String> extractWords(Document doc) {
		Vector<String> result = new Vector<String>();
		String contents = doc.body().text();
		StringTokenizer st = new StringTokenizer(contents);
		while (st.hasMoreTokens()) {
			result.add(st.nextToken());
		}
		return result;
	}

	/*
	 * Extract useful external urls on the web page. note: filter out images,
	 * emails, etc.
	 * 
	 * @param {Document} doc
	 * 
	 * @return {Vector<String>} a list of external links on the web page
	 */
	public Vector<String> extractLinks(Document doc) {
		Vector<String> result = new Vector<String>();
		// ADD YOUR CODES HERE
		Elements links = doc.select("a[href]");
		for (Element link : links) {
			String linkString = link.attr("href");
			// filter out emails and empty and valueless hrefs
			if (linkString.contains("mailto:") || linkString.contains("#") || linkString.contains("javascript:")
					|| linkString.equals("")) {
				continue;
			}
			result.add(link.attr("href"));
		}
		return result;
	}

	/*
	 * Print all info about the focused page to console
	 */
	public void printPageInfo(Response res, Document doc, Link focus) {
		System.out.println("Page title: " + doc.title());
		System.out.println("URL: " + focus.url);
		System.out.printf("Last Modified: %s\t", res.header("last-modified"));
		System.out.printf("Page Size: %d Bytes\n", res.bodyAsBytes().length);
	}

	/*
	 * Print page's words and links
	 */
	public void printWordsAndLinks(Link focus, Set<Entry<String, Integer>> keywordFreqPair, Vector<String> links,
			Document doc) {
		System.out.println("\nWords:");
		for (Entry<String, Integer> entry : keywordFreqPair) {
			System.out.print(entry.getKey() + " " + entry.getValue() + "; ");
		}
		System.out.println("\n\nLinks:");

		int index = 1;
		for (String link : links) {
			System.out.println(String.format("Child Link %d: %s", index++, link));
			// Check if link has cralwed or not
			if (this.urls.contains(link)) {
				continue;
			}
			// Check if the child link is the relative path
			else if (focus.url.contains(this.domain) && link.charAt(0) == '/') {
				String actual_url = handleSpecialURL(focus.url, link);
				if (this.urls.contains(actual_url)) {
					continue;
				}
				this.URLqueue.add(new Link(actual_url, focus.level + 1)); // add links
			}
			// Check if the child link is belongs to cse domain
			else if (link.contains(this.domain)) {
				this.URLqueue.add(new Link(link, focus.level + 1)); // add links
			} else {
				continue;
			}
		}
		System.out
				.println("-------------------------------------------------------------------------------------------");
	}

	/**
	 * Use a URLqueue to manage crawl tasks.
	 */
	public void crawlLoop(InvertedIndex index) {
		int count = 0;

		// To map the first link (i.e. https://www.cse.ust.hk/)
		Vector<String> head_link = new Vector<String>();
		head_link.add(this.first_url);
		docMapping(this.first_url, head_link, index);

		while (!this.URLqueue.isEmpty()) {
			Link focus = this.URLqueue.remove(0);
			/* start to crawl on the page */
			try {
				if (count++ == 10)
					break; // stop criteria
				Response res = this.getResponse(focus, index);
				Document doc = res.parse();

				if (doc.title().isEmpty() || res.statusCode() == 301 || res.statusCode() == 302
						|| res.statusCode() == 404 || res.statusCode() == 403 || res.statusCode() == 401) {
					continue;
				}

				Vector<String> words = this.extractWords(doc);
				Vector<String> links = this.extractLinks(doc);

				// handle Chinese characters
				Iterator<String> iter = words.iterator();
				while (iter.hasNext()) {
					String str = iter.next();
					if (str.matches("[\\u4E00-\\u9FA5]+"))
						iter.remove();
				}

				// stemming before passing
				StopStem stopStem = new StopStem("lib/stopwords-en.txt");
				stopStem.importSource(words);
				words = stopStem.StemWord();
				Collections.sort(words);

				Set<Entry<String, Integer>> keywordFreqPair = forwardIndex(focus.url, words, index);

				// Print spider_result fashion output to console
				printPageInfo(res, doc, focus);
				printWordsAndLinks(focus, keywordFreqPair, links, doc);

				// Creating URL and docID Mapping
				docMapping(focus.url, links, index);

				// Creating Word and WordID Mapping
				wordMapping(words, index);

				// Creating Inverted File
				invertedIndexing(focus.url, keywordFreqPair, index);

				// Creating Parent-Child Relationship
				parentChild(focus.url, links, index);

			} catch (Exception e) {
				count--;
				continue;
			}
		}
	}

	/*
	 * URL Helper (handle link starting with "/"
	 */
	public String handleSpecialURL(String parent, String link) {
		if (parent.contains(this.domain) && link.charAt(0) == '/') {
			int pos = parent.indexOf(this.domain);
			return parent.substring(0, pos + this.domain.length() + 1) + link.substring(1);
		}
		return link;
	}

	/*
	 * Inverted Indexing
	 */
	public void invertedIndexing(String url, Set<Entry<String, Integer>> keywordFreqPair, InvertedIndex index) {
		for (Entry<String, Integer> entry : keywordFreqPair) {
			try {
				index.addInvertedIndex(url, entry.getKey(), entry.getValue());
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/*
	 * Document Mapping
	 */
	public void docMapping(String p_url, Vector<String> links, InvertedIndex index) {
		for (String url : links) {
			try {
				url = handleSpecialURL(p_url, url);
				index.addDocMappingEntry(url);
			} catch (RocksDBException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * Word Mapping
	 */
	public void wordMapping(Vector<String> words, InvertedIndex index) {
		for (String word : words) {
			try {
				index.addWordMappingEntry(word);
			} catch (RocksDBException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * Add Linkage
	 */
	public void parentChild(String p_url, Vector<String> children, InvertedIndex index) {
		for (String child_url : children) {
			try {
				child_url = handleSpecialURL(p_url, child_url);
				if (p_url.contains("cse.ust.hk") && child_url.contains("cse.ust.hk")) {
					index.addPCRelation(p_url, child_url);
				} else {
					continue;
				}
			} catch (RocksDBException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * Forward Indexing
	 */
	public Set<Entry<String, Integer>> forwardIndex(String url, Vector<String> words, InvertedIndex index) {
		// forward_docID -> (word, freq)
		Map<String, Integer> wordAndCount = new HashMap<String, Integer>();

		for (String word : words) {
			Integer count = wordAndCount.get(word);

			if (count == null) {
				wordAndCount.put(word, 1);
			} else {
				wordAndCount.put(word, ++count);
			}
		}

		Set<Entry<String, Integer>> entrySet = wordAndCount.entrySet();
		for (Entry<String, Integer> entry : entrySet) {
			try {
				index.addForwardIndex(url, entry.getKey(), entry.getValue());
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return entrySet;
	}

	/*
	 * rocksdb connection helper
	 */
	public static InvertedIndex RocksDBConnection() {
		InvertedIndex index = null;
		try {
			// a static method that loads the RocksDB C++ library
			RocksDB.loadLibrary();

			String path = "db";

			index = new InvertedIndex(path);
			// index.clearAll();
		} catch (RocksDBException e) {
			System.err.println(e.toString());
		}
		return index;
	}

	public String getInfo(Crawler crawler, InvertedIndex index) {
		String result = "";
		// int count = 0;
		try {
			HashSet<String> urls = crawler.urls;
			for (String url : urls) {
				// count++;
				// result = result + String.valueOf(count) + index.getDocIDbyURL(url) + " Page
				// Title: " + index.getTitlebyURL(url) + "\n";

				// get the page title
				result = result + "Page Title: " + index.getTitlebyURL(url) + "\n";

				// get the url
				result = result + "URL: " + url + "\n";

				// get last modification date
				result = result + "Last Modification Date: " + index.getLastModifiedbyURL(url);

				// get page size
				result = result + ", Size of page: " + index.getSizebyURL(url) + "\n\n";

				// get forward indexing keyword freq
				result = result + index.getForwardIndex(url);

				// get the child link
				result = result + index.getPCRelation(url);

				result = result + "------------------------------------------------------------------\n";
			}
		} catch (RocksDBException e) {
			System.err.println(e.toString());
		}
		return result;
	}

	public void outputTXT(String content) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("./spider_result.txt"));
			writer.write(content);
			writer.close();
			System.out.println("Output spider_result.txt successfully");
		} catch (IOException e) {
			System.err.println(e.toString());
		}
	}

	public static void main(String[] args) {
		InvertedIndex index = RocksDBConnection();
		Crawler crawler = new Crawler("https://www.cse.ust.hk/");
		boolean continued = true;
		// Ask for Action
		while (continued) {
			Scanner sc = new Scanner(System.in); // System.in is a standard input stream
			System.out.println("============================");
			System.out.println("What do you want?");
			System.out.println("Start Crawling => 1");
			System.out.println("Start Query => 2");
			System.out.println("Update PR + Print PR => 3");
			System.out.println("Print Database => 4");
			System.out.println("Clear Database => 5");
			System.out.println("End Program => 0");
			System.out.println("============================");
			System.out.print("Enter your choice - ");
			int choice = sc.nextInt();

			switch (choice) {
				case 0:
					continued = false;
					System.out.println("Closing DB...");
					index.closeAllDB();
					break;
				case 1:
					System.out.println("Crawling Website...");
					crawler.crawlLoop(index);
					break;
				case 2:
					try {
						System.out.print("Please input your query");
						String inputQuery = sc.nextLine();
						index.rankingAlgorithm(inputQuery, 100);
					} catch (RocksDBException e) {
						e.printStackTrace();
					}
					break;
				case 3:
					try {
						System.out.println("PageRank Updating...");
						index.updatePageRankIntoDB(0.8, 10, true);
						index.printPageRankArray();
					} catch (RocksDBException e) {
						e.printStackTrace();
					}
					break;
				case 4:
					try {
						index.printAll();
					} catch (RocksDBException e) {
						e.printStackTrace();
					}
					break;
				case 5:
					try {
						index.clearAll();
					} catch (RocksDBException e) {
						e.printStackTrace();
					}
					break;
				default:
					System.out.println("Please input a valid choice.");
			}
		}
		String final_output = crawler.getInfo(crawler, index);
		crawler.outputTXT(final_output);
		System.out.println("\nSuccessfully Returned");

	}

};
package mypackage;

import org.rocksdb.*;

public class DbHandler {

	private RocksDB docmapdb;
	private RocksDB wordmapdb;
	private RocksDB inverteddb;
	private RocksDB forwarddb;
	private RocksDB metadatadb;
	private RocksDB parentchilddb;
	private RocksDB pagerankdb;

	private Status s1;
	private String absolute = "/Users/anthonykwok/Documents/Academic/HKUST/Year 2020-2021 (DSCT Yr 3)/2021 Spring Semester Course/COMP4321/Project/4321-repo/apache-tomcat-10.0.5/";
	private String db1 = absolute + "webapps/example/db/doc";
	private String db2 = absolute + "webapps/example/db/word";
	private String db3 = absolute + "webapps/example/db/invert";
	private String db4 = absolute + "webapps/example/db/forward";
	private String db5 = absolute + "webapps/example/db/metadata";
	private String db6 = absolute + "webapps/example/db/parentchild";
	private String db7 = absolute + "webapps/example/db/pagerank";

	public RocksDB db;
	private Options options;

	public DbHandler(String dbPath) throws RocksDBException {
		RocksDB.loadLibrary();
		this.options = new Options();
		this.options.setCreateIfMissing(true);
		try {
			this.db = RocksDB.open(options, dbPath);
		} catch (RocksDBException e) {
			System.err.println(e.toString());
		}

	}

	public String sayHi() {
		return "Hi!";
	}

	public void close() {
		this.db.close();
	}

}

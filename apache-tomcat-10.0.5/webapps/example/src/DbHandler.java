package mypackage;

import org.rocksdb.*;

public class DbHandler {
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

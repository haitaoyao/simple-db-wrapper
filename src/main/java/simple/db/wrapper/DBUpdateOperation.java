package simple.db.wrapper;


public abstract class DBUpdateOperation extends DBOperation {

	public DBUpdateOperation(String sql) {
		super(sql);
	}
}

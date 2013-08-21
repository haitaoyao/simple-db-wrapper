package simple.db.wrapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * query db
 * 
 * @author haitao
 * 
 * @param <T>
 */
public abstract class DBQueryOperation<T> extends DBOperation {

	public DBQueryOperation(String sql) {
		super(sql);
	}

	public abstract T parseResultSet(ResultSet rs) throws SQLException;
}

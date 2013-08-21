package simple.db.wrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.google.common.base.Preconditions;

public abstract class DBOperation {

	private final String sql;

	public DBOperation(String sql) {
		Preconditions.checkNotNull(sql);
		this.sql = sql;
	}

	public final String getSql() {
		return this.sql;
	}

	public abstract void setParameter(PreparedStatement ps) throws SQLException;
}

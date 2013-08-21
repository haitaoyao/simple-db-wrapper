package simple.db.wrapper;

import java.sql.SQLException;

public class QueryExceedLimitSQLException extends SQLException {

	private static final long serialVersionUID = 1233330753924486439L;

	public QueryExceedLimitSQLException(String message) {
		super(message);
	}
}

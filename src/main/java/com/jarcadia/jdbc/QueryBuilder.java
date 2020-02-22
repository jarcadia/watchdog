package com.jarcadia.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

public class QueryBuilder {
	
	private final QueryRunner runner;
	private final String sql;
	private final Object[] params;
	
	protected QueryBuilder(QueryRunner runner, String sql, Object[] params) {
		this.runner = runner;
		this.sql = sql;
		this.params = params;
	}
	
	public <T> T andReturnValue(ValueMapper<T> mapper) throws SQLException {
		ResultSetHandler<T> handler = new ResultSetHandler<>() {
			@Override
			public T handle(ResultSet rs) throws SQLException {
				T result = null;
				if (rs.next()) {
                    result = mapper.map(rs);
				} else {
					throw new RuntimeException("No rows to map");
				}
				
				if (rs.next()) {
					throw new RuntimeException("Expected single row");
				}
				return result;
			}
		};
		return runner.query(sql, handler, params);
	}
	
	@FunctionalInterface
	public interface ValueMapper<Value> {
		
		public Value map(ResultSet rs) throws SQLException;
	}

}

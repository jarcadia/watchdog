package com.jarcadia.jdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

public abstract class JarcadiaJdbcService<Key> {

	private final Map<Key, QueryRunner> queryRunnerMap;
	
	public JarcadiaJdbcService() {
		this.queryRunnerMap = new ConcurrentHashMap<>();
	}

	public abstract DataSource initDataSource(Key key) throws Exception;
	
	public QueryBuilder query(Key key, String query, Object... params) {
		if (key == null) {
			throw new IllegalArgumentException("Null key is not allowed");
		}
		QueryRunner runner = queryRunnerMap.computeIfAbsent(key, k -> new QueryRunner(this.safeInit(key)));
		return new QueryBuilder(runner, query, params);
	}
	
	public int execute(Key key, String stmt, Object... params) throws SQLException {
		QueryRunner runner = queryRunnerMap.computeIfAbsent(key, k -> new QueryRunner(this.safeInit(key)));
		return runner.update(stmt, params);
	}
	
	private DataSource safeInit(Key key) {
		try {
			DataSource ds = this.initDataSource(key);
			if (ds == null) {
				throw new RuntimeException("Invalid null DataSource for key " + key.toString());
			}
			return ds;
		} catch (Exception e) {
			throw new RuntimeException("Unable to initialize datasource for " + key.toString(), e);
		}
	}
	
	
	
	
}

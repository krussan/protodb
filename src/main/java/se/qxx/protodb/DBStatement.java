package se.qxx.protodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.Descriptors.FieldDescriptor;

public class DBStatement {
	private FieldDescriptor matchingField;
	private PreparedStatement statement;
	private Connection connection;
	private int parameterCounter = 1;
	
	public FieldDescriptor getMatchingField() {
		return matchingField;
	}
	public Connection getConnection() {
		return connection;
	}
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public void setMatchingField(FieldDescriptor matchingField) {
		this.matchingField = matchingField;
	}
	public PreparedStatement getStatement() {
		return statement;
	}
	public void setStatement(PreparedStatement statement) {
		this.statement = statement;
	}
	
	public DBStatement(Connection conn) {
		this.setConnection(conn);
	}
	
	public DBStatement(FieldDescriptor matchingField, String dbQuery, Connection conn) throws SQLException {
		this.setMatchingField(matchingField);
		this.setConnection(conn);
		
		PreparedStatement prep = conn.prepareStatement(dbQuery);
		this.setStatement(prep);
	}
	
	public void prepareStatement(String dbQuery) throws SQLException {
		PreparedStatement prep = this.getConnection().prepareStatement(dbQuery);
		this.setStatement(prep);		
	}
	
	public void addString(String string) throws SQLException {
		this.getStatement().setString(this.parameterCounter, string);
	}
	
	public void addObject(Object o) throws SQLException {
		this.getStatement().setObject(this.parameterCounter, o);
	}
	public ResultSet executeQuery() throws SQLException {
		return this.getStatement().executeQuery();
	}
}

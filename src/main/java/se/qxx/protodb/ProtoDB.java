package se.qxx.protodb;

import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import se.qxx.protodb.backend.ColumnDefinition;
import se.qxx.protodb.backend.DatabaseBackend;
import se.qxx.protodb.exceptions.IDFieldNotFoundException;
import se.qxx.protodb.exceptions.ProtoDBParserException;
import se.qxx.protodb.exceptions.SearchFieldNotFoundException;
import se.qxx.protodb.model.ProtoDBSearchOperator;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.mysql.jdbc.DatabaseMetaData;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtoDB {
	private boolean populateBlobs = true;
	private DatabaseBackend databaseBackend;
	
	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  PROPS
	//---------------------------------------------------------------------------------
	
	public DatabaseBackend getDatabaseBackend() {
		return databaseBackend;
	}

	public void setDatabaseBackend(DatabaseBackend databaseBackend) {
		this.databaseBackend = databaseBackend;
	}

	public boolean isPopulateBlobsActive() {
		return populateBlobs;
	}

	public void setPopulateBlobs(boolean populateBlobs) {
		this.populateBlobs = populateBlobs;
	}
	
	//---------------------------------------------------------------------------------
	//------------------------------------------------------------------ CONSTRUCTORS
	//---------------------------------------------------------------------------------
	
	protected ProtoDB(DatabaseBackend backend, String logFilename) {
		this.setDatabaseBackend(backend);
		Logger.setLogfile(logFilename);
	}
	
	protected ProtoDB(String logFilename) {
		Logger.setLogfile(logFilename);
	}	

	public List<ColumnDefinition> retreiveColumns(String table) throws SQLException, ClassNotFoundException {
		List<ColumnDefinition> result = new ArrayList<ColumnDefinition>();
		Connection conn = null;
		try {
			conn = this.initialize();
			
			result = this.getDatabaseBackend().getColumns(table, conn);
		}
		catch (Exception e) {
			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();
			
			throw e;
		}
		finally {
			this.disconnect(conn);
		}
		
		return result;
	}
	
	/***
	 * Database function for retrieving column information 
	 * @param table
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private ResultSet retreiveColumns(String table, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(String.format("PRAGMA table_info('%s')", table));
		
		return prep.executeQuery();
	}


	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  INIT
	//---------------------------------------------------------------------------------
	
	/***
	 * Initializes a database connection
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	protected Connection initialize() throws ClassNotFoundException, SQLException {
		Class.forName(this.getDatabaseBackend().getDriver());
	    return DriverManager.getConnection(this.getDatabaseBackend().getConnectionString());				
	}
		
	/***
	 * Disconnects the database and the conneciton object
	 * @param conn
	 */
	private void disconnect(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
		}
	}

	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  SETUP
	//---------------------------------------------------------------------------------

	/***
	 * Purges the database from all tables and sets up the whole database structure from
	 * one given protobuf class.
	 * @param b
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IDFieldNotFoundException 
	 */
	public void setupDatabase(MessageOrBuilder b) throws SQLException,ClassNotFoundException, IDFieldNotFoundException {
		Connection conn = null;
		
		try {
			conn = this.initialize();
			conn.setAutoCommit(false);
			
			this.setupDatabase(b, conn);
			
			conn.commit();
		}
		catch (SQLException e) {			
			try {
				if (conn != null)
					conn.rollback();
				
			} catch (SQLException sqlEx) {}

			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();

			throw e;
		}
		finally {
			this.disconnect(conn);
		}
	}
	
	
	/**
	 * Purges the database from all tables and sets up the whole database structure from
	 * one given protobuf class.
	 * @throws SQLException 
	 * @throws IDFieldNotFoundException 
	 * @throws UnexpectedException 
	 */
	private void setupDatabase(MessageOrBuilder b, Connection conn) throws SQLException, IDFieldNotFoundException {
		ProtoDBScanner scanner = new ProtoDBScanner(b, this.getDatabaseBackend());
		
		// check fields for ID field - this has to be present
		
		if (!idFieldExists(scanner))
			throw new IDFieldNotFoundException(scanner.getObjectName());
		
		// setup all sub objects
		setupSubObjects(b, conn, scanner);
		
		// setup blob data if blobs exist
		if (scanner.getBlobFields().size() > 0)
			setupBlobdata(conn);
			
		// setup this object
		if (!tableExist(scanner.getObjectName(), conn)) {
			executeStatement(
					scanner.getCreateStatement(this.getDatabaseBackend()), 
					conn);
		}
		
		// setup all repeated fields as many-to-many relations
		setupRepeatedObjects(conn, scanner);
		
		for (FieldDescriptor field : scanner.getRepeatedBasicFields()) {
			executeStatement(scanner.getBasicLinkCreateStatement(field), conn);
		}

	}

	private void setupRepeatedObjects(Connection conn, ProtoDBScanner scanner) throws SQLException, IDFieldNotFoundException {
		for(FieldDescriptor field : scanner.getRepeatedObjectFields()) {
			if (field.getJavaType() == JavaType.MESSAGE) {
				Message mg = getInstanceFromField(field);
				
				if (mg instanceof MessageOrBuilder) {
					MessageOrBuilder b2 = (MessageOrBuilder)mg;
					
					// create other object
					setupDatabase(b2, conn);
					
					// create link table
					ProtoDBScanner other = new ProtoDBScanner(b2, this.getDatabaseBackend());
					if (!tableExist(scanner.getLinkTableName(other, field.getName()), conn))
						executeStatement(scanner.getLinkCreateStatement(other, field.getName()), conn);
				}
			}
			else if (field.getJavaType() == JavaType.ENUM) {
				setupDatabase(field.getEnumType(), conn);
				
				if (!tableExist(scanner.getEnumLinkTableName(field), conn)) {
					PreparedStatement prep = conn.prepareStatement(
						scanner.getEnumLinkCreateStatement(field));
				
					prep.execute();
				}
			}
		}
	}

	private void setupSubObjects(MessageOrBuilder b, Connection conn, ProtoDBScanner scanner)
			throws SQLException, IDFieldNotFoundException {
		for(FieldDescriptor field : scanner.getObjectFields()) {
			if (!field.isRepeated()) {
				if (field.getJavaType() == JavaType.MESSAGE)
					setupDatabase((MessageOrBuilder)b.getField(field), conn);
				else if (field.getJavaType() == JavaType.ENUM){
					setupDatabase(field.getEnumType(), conn);
				}
			}
		}
	}

	private Boolean idFieldExists(ProtoDBScanner scanner) {
		Boolean idFieldFound = false;
		for (FieldDescriptor field : scanner.getBasicFields()) {
			if (field.getName().equalsIgnoreCase("ID") 
					&& field.getJavaType() == JavaType.INT
					&& field.isRequired()) {
				idFieldFound = true;
				break;
			}
		}
		return idFieldFound;
	}
	
	private void setupDatabase(EnumDescriptor fieldName, Connection conn) throws SQLException {
		String tableName = StringUtils.capitalize(fieldName.getName());
		if (!tableExist(tableName, conn)) {
			String sql = String.format(
				"CREATE TABLE %s(%s,value TEXT NOT NULL)"
					, tableName
					, this.getDatabaseBackend().getIdentityDefinition());

			Logger.log(sql);
			
			PreparedStatement prep = conn.prepareStatement(sql);
			prep.execute();
			
			for (EnumValueDescriptor value : fieldName.getValues()) {
				sql = "INSERT INTO " + tableName + " (value) VALUES (?)";
				Logger.log(sql);
				
				prep = conn.prepareStatement(sql);
				prep.setString(1, value.getName());
				
				prep.execute();
			}
		}
	}

	private void setupBlobdata(Connection conn) throws SQLException {
		if (!tableExist("BlobData", conn))
			executeStatement(
				String.format("CREATE TABLE BlobData (%s, data BLOB)", 
						this.getDatabaseBackend().getIdentityDefinition()), 
						conn);
	}

	private void executeStatement(String sql, Connection conn) throws SQLException {
		Logger.log(sql);
		
		PreparedStatement prep = conn.prepareStatement(sql);
		prep.execute();			
	}
	
	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  GET
	//---------------------------------------------------------------------------------

	
	public <T extends Message> T get(int id, T instance) throws ClassNotFoundException, SQLException{
		return this.get(id, new ArrayList<String>(), instance);
	}
	
	/***
	 * 
	 * @param <T>
	 * @param id
	 * @param excludedObjects
	 * @param desc
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public <T extends Message> T get(int id, List<String> excludedObjects, T instance) throws ClassNotFoundException, SQLException{
		Connection conn = null;
		T msg = null;
		
		try {
			conn = this.initialize();
			
			msg = get(id, excludedObjects, instance, conn);
			
		}
		catch (Exception e) {
			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();
			
			throw e;
		}		
		finally {
			this.disconnect(conn);
		}		
		
		return msg;
	}

 	/***
	 * 
	 * @param id
	 * @return
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	<T extends Message> T get(int id, List<String> excludedObjects, T instance, Connection conn) throws SQLException{
		Builder b = instance.newBuilderForType();
		
		ProtoDBScanner scanner = new ProtoDBScanner(instance, this.getDatabaseBackend());
		Logger.log(String.format("Populating object %s :: %s", scanner.getObjectName(), id));
		
		// populate list of sub objects
		Populator.populateRepeatedObjectFields(this, id, excludedObjects, conn, b, scanner);

		// populate list of basic types
		Populator.populateRepeatedBasicFields(id, conn, b, scanner);			
		
		ResultSet rs = getResultSetForObject(id, conn, b, scanner);
		
		int rowcount = 0;
 		while(rs.next()) {
			// populate object fields
			Populator.populateObjectFields(this, conn, b, scanner, rs, excludedObjects);
			
			// populate blobs		
			if (this.isPopulateBlobsActive())
				Populator.populateBlobs(conn, b, scanner, rs);
			
			// populate basic fields			
			Populator.populateBasicFields(id, b, scanner, rs);	
			
			rowcount++;
		}
		
		if (rowcount>0)
			return (T) b.build();
		else
			return null;
	}

	private ResultSet getResultSetForObject(int id, Connection conn, Builder b, ProtoDBScanner scanner)
			throws SQLException {
		String sql = scanner.getSelectStatement(id);
		Logger.log(sql);
		
		PreparedStatement prep = conn.prepareStatement(sql);
		prep.setInt(1, id);
		b.setField(scanner.getIdField(), id);
		
		ResultSet rs = prep.executeQuery();
		return rs;
	}
	
	/***
	 * Function that retrieves recursively all the linked objects
	 * from the database. Assumes that a shallow copy of the objects have been populated.
	 * 
	 * @param listOfObjects
	 * @return
	 */
	public <T extends Message> List<T> getByJoin(List<T> listOfObjects, boolean populateBlobs) throws ClassNotFoundException, SQLException, ProtoDBParserException {
		
		if (listOfObjects != null && listOfObjects.size() > 0) {
			
			Connection conn = null;
			
			try {
				conn = this.initialize();
			
				T instance = listOfObjects.get(0);
				ProtoDBScanner scanner = new ProtoDBScanner(instance, this.getDatabaseBackend());;
	
				// get a list of all parent id's
				List<Integer> ids = new ArrayList<Integer>();
				for (T message : listOfObjects ) {
					ids.add((int)message.getField(scanner.getIdField()));
				}
				
				// shallow copy exits. Loop through object fields
				// and create a shallow copy for that field.
				// map the results to the parent object
				for (FieldDescriptor field : scanner.getRepeatedObjectFields()) {
					DynamicMessage innerInstance = getInstanceFromField(field);
					JoinResult joinResult = getLinkJoinResult(ids, scanner, field, populateBlobs);
				
					PreparedStatement prep = joinResult.getStatement(conn);
					ResultSet rs = prep.executeQuery();
					
					Map<Integer, List<DynamicMessage>> result = joinResult.getResultLink(innerInstance, rs, this.isPopulateBlobsActive());
					return updateParentObjects(scanner, field, listOfObjects, result);
				}
			
			}
			catch (Exception e) {
				System.out.println("Exception in ProtoDB!");
				e.printStackTrace();
				
				throw e;
			}		
			finally {
				this.disconnect(conn);
			}
			
		}
		
		return null;
			
	}
	
	@SuppressWarnings("unchecked")
	private <T extends Message> List<T> updateParentObjects(ProtoDBScanner parentScanner, FieldDescriptor field, List<T> listOfObjects, Map<Integer, List<DynamicMessage>> result) {
		List<T> parents = new ArrayList<T>();
		for (T obj : listOfObjects) {
			int parentID = (int)obj.getField(parentScanner.getIdField());
			List<DynamicMessage> subObjects = result.get(parentID);
			
			Builder b = obj.toBuilder();
			
			if (subObjects != null) {
				for (DynamicMessage sub : subObjects) {
					b.addRepeatedField(field, sub);
				}
			}
			
			parents.add((T)b.build());
		}
		
		return parents;
	}
	
	private <T extends Message> JoinResult getLinkJoinResult(List<Integer> parentIDs, ProtoDBScanner scanner, FieldDescriptor field, boolean populateBlobs) {
		if (field.getJavaType() == JavaType.MESSAGE) {
			Message mg = getInstanceFromField(field);
			
			if (mg instanceof MessageOrBuilder) {

				ProtoDBScanner other = new ProtoDBScanner(mg, this.getDatabaseBackend());;
				JoinResult joinResult = Searcher.getJoinQuery(other, populateBlobs, false, scanner, field.getName(), -1, -1);
				
				joinResult.addLinkWhereClause(parentIDs, scanner);
				
				return joinResult;
			}
		}
		
		return null;
	}

	private DynamicMessage getInstanceFromField(FieldDescriptor field) {
		Descriptor mt = field.getMessageType();
		return DynamicMessage.getDefaultInstance(mt);
	}

	/***
	 * Retrieves a list of objects based on their ID's using joins instead of 
	 * repeated queries for each object (as opposed to the get function).
	 * Complex models needs several queries to get the data (i.e. repeated objects)
	 * 
	 * @param instance
	 * @param ids
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws SearchFieldNotFoundException
	 */
	public <T extends Message> List<T> getByJoin(T instance, List<Integer> ids) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, "ID", StringUtils.join(ids, ","), ProtoDBSearchOperator.In, true);
	}
	
	/***
	 * Retrieves an object based on the ID using joins instead of 
	 * repeated queries for each object (as opposed to the get function).
	 * Complex models needs several queries to get the data (i.e. repeated objects)
	 * 
	 * @param instance
	 * @param id
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws SearchFieldNotFoundException
	 */
	public <T extends Message> List<T> getByJoin(T instance, int id) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return getByJoin(instance, Arrays.asList(id));
	}	




	
	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  SAVE
	//---------------------------------------------------------------------------------


	/***
	 * Saves a protobuf class to database.
	 * @param b
	 * @return 
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public <T extends Message> T save(T b) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		T nb;
		
		try {
			conn = this.initialize();
			conn.setAutoCommit(false);
			
			nb = this.save(b, conn);
			
			conn.commit();
		}
		catch (SQLException e) {			
			try {
				if (conn != null)
					conn.rollback();

			} catch (SQLException sqlEx) {}

			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();

			throw e;
		}		
		finally {
			this.disconnect(conn);
		}
		
		return nb;

	}

	/***
	 * Internal save function. Saves a protobuf class to database.
	 * @param b
	 * @param conn
	 * @return 
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	private <T extends Message> T save(T b, Connection conn) throws SQLException, ClassNotFoundException {
		// create a new builder to store the new object
		Builder mainBuilder = b.newBuilderForType();
		ProtoDBScanner scanner = new ProtoDBScanner(b, this.getDatabaseBackend());;
		
		//check for existence. UPDATE if present!
		Boolean objectExist = checkExisting(scanner, conn);
		
		// getObjectFields
		// getBasicFields
		// getRepeatedObjectFields
		// getRepeatedBasicFIelds
		
		// save underlying objects
		for(FieldDescriptor field : scanner.getObjectFields()) {
			String fieldName = field.getName();
			Object o = b.getField(field);
			
			if (field.getJavaType() == JavaType.MESSAGE && !field.isRepeated()) {
				ProtoDBScanner other = new ProtoDBScanner((Message)o, this.getDatabaseBackend());;
				Message ob = save((Message)o, conn);				
				scanner.addObjectID(fieldName, (int)ob.getField(other.getIdField()));
				
				//subBuilder.setField(other.getIdField(), objectID);
				mainBuilder.setField(field, ob);
				
			}
			else if (field.getJavaType() == JavaType.ENUM && !field.isRepeated()) {
				int enumID = saveEnum(field, ((EnumValueDescriptor)o).getName(), conn);
				mainBuilder.setField(field, ((EnumValueDescriptor)o));
				scanner.addObjectID(fieldName, enumID);
			}
		}
		

		// delete blobs
		if (objectExist)
			deleteBlobs(scanner, conn);

		// save blobs
		for(FieldDescriptor field : scanner.getBlobFields()) {
			String fieldName = field.getName();
			
			ByteString bs = (ByteString)b.getField(field);
			int blobID = saveBlob(bs.toByteArray(), conn);
			scanner.addBlobID(fieldName, blobID);
			mainBuilder.setField(field, bs);
		}		
		
		// save this object
		int thisID = saveThisObject(b, scanner, objectExist, conn);
		mainBuilder.setField(scanner.getIdField(), thisID);
		
		// populate basic fields in return message
		for(FieldDescriptor field : scanner.getBasicFields())
			if (!field.getName().equalsIgnoreCase("ID"))
				mainBuilder.setField(field, b.getField(field));
		
		// save underlying repeated objects
		for(FieldDescriptor field : scanner.getRepeatedObjectFields()) {
			// for each repeated field get insert statement according to _this_ID, _other_ID
			int fieldCount = b.getRepeatedFieldCount(field);
			for (int i=0;i<fieldCount;i++) {
				Object mg = b.getRepeatedField(field, i);
				if (mg instanceof Message) {
					Message b2 = (Message)mg;
					ProtoDBScanner other = new ProtoDBScanner(b2, this.getDatabaseBackend());;
					
					// save other object
					Message ob = save(b2, conn);
					
					// delete from link table
					deleteLinkObject(scanner, other, field, conn);
					
					// save link table
					int otherID = (int)ob.getField(other.getIdField());
					saveLinkObject(scanner, other, field, thisID, otherID, conn);
					
					mainBuilder.addRepeatedField(field, ob);
				}
			}
		}
		
		// save underlying repeated basic types
		for (FieldDescriptor field : scanner.getRepeatedBasicFields()) {
			// delete from link table
			deleteBasicLinkObject(scanner, field, conn);
			
			// add each value to link table
			int fieldCount = b.getRepeatedFieldCount(field);
			for (int i=0;i<fieldCount;i++) {
				Object value = b.getRepeatedField(field, i);
				saveLinkBasic(scanner, thisID, field, value, conn);
				mainBuilder.addRepeatedField(field, value);
			}			
		}
				
		return (T)mainBuilder.build();
	}
	
	private void deleteBasicLinkObject(ProtoDBScanner scanner, FieldDescriptor field, Connection conn) throws SQLException {
		String sql = scanner.getBasicLinkTableDeleteStatement(field);
		Logger.log(sql);
		
		PreparedStatement prep = conn.prepareStatement(sql);
		prep.setInt(1, scanner.getIdValue());
		
		prep.execute();
	}

	private void deleteLinkObject(ProtoDBScanner scanner, 
			ProtoDBScanner other,
			FieldDescriptor field,
			Connection conn) throws SQLException {
		String sql = scanner.getLinkTableDeleteStatement(other, field.getName());
		Logger.log(sql);
		
		PreparedStatement prep = conn.prepareStatement(sql);
		prep.setInt(1, scanner.getIdValue());
		prep.setInt(2, other.getIdValue());
		
		prep.execute();
	}
	
	private void deleteRow(ProtoDBScanner scanner, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(scanner.getDeleteStatement());
		prep.setInt(1, scanner.getIdValue());
		
		prep.execute();
	}

	private void deleteBlobs(ProtoDBScanner scanner, Connection conn) throws SQLException {
		for (FieldDescriptor field : scanner.getBlobFields()) {
			String sql = "DELETE FROM BlobData WHERE ID IN (SELECT " + scanner.getObjectFieldName(field) + " FROM " + scanner.getObjectName() + " WHERE ID = ?)";
			Logger.log(sql);
			
			PreparedStatement prep = conn.prepareStatement(sql);
			prep.setInt(1, scanner.getIdValue());
			
			prep.execute();
		}		
	}
	
	private Boolean checkExisting(ProtoDBScanner scanner, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT COUNT(*) FROM " + scanner.getObjectName() + " WHERE ID = ?");
		prep.setInt(1, scanner.getIdValue());
		
		ResultSet rs = prep.executeQuery();
		Boolean exists = false;
		if (rs.next())
			exists = rs.getInt(1) > 0;
			
		rs.close();
		prep.close();
		return exists;
	}

	private int saveBlob(byte[] data, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement("INSERT INTO BlobData (data) VALUES (?)");
		prep.setBytes(1, data);
		prep.execute();
		
		return this.getDatabaseBackend().getIdentityValue(conn);
	}
	
	private int saveEnum(FieldDescriptor field, String value, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement(
			String.format("SELECT ID FROM %s WHERE value = ?", 
				StringUtils.capitalize(field.getName())));
		
		prep.setString(1, value);
		ResultSet rs = prep.executeQuery();
		
		if (rs.next())
			return rs.getInt(1);
		else
			return -1;
		
	}

	/***
	 * Saves a repeated list of basic types to link table
	 * @param scanner
	 * @param thisID
	 * @param field
	 * @param value
	 * @param conn
	 * @throws SQLException
	 */
	protected void saveLinkBasic(ProtoDBScanner scanner
			, int thisID
			, FieldDescriptor field
			, Object value
			, Connection conn)
			throws SQLException {
		
		
		String sql = scanner.getBasicLinkInsertStatement(field);
		Logger.log(sql);
		
		PreparedStatement prep = 
			scanner.compileLinkBasicArguments(
					  sql
					, thisID
					, field.getJavaType()
					, value
					, conn);
		
		prep.execute();
	}

	/***
	 * Saves a repeated list of objects to link table (many-to-many)
	 * @param b2
	 * @param scanner
	 * @param field
	 * @param thisID
	 * @param otherID
	 * @param conn
	 * @throws SQLException
	 */
	private void saveLinkObject(
			  ProtoDBScanner scanner
			, ProtoDBScanner other
			, FieldDescriptor field
			, int thisID
			, int otherID
			, Connection conn) throws SQLException {
		scanner.getLinkTableInsertStatement(other, field.getName());
		
		PreparedStatement prep = conn.prepareStatement(scanner.getLinkTableInsertStatement(other, field.getName()));
		prep.setInt(1, thisID);
		prep.setInt(2, otherID);
		
		prep.execute();
	}
	
	/***
	 * Saves the original object (without references to other objects or lists)
	 * @param b
	 * @param scanner
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private int saveThisObject(MessageOrBuilder b, ProtoDBScanner scanner, Boolean objectExist, Connection conn) throws SQLException {
		// getInsertStatement
		String sql = scanner.getSaveStatement(objectExist);
		
		// prepareStatement
		PreparedStatement prep = scanner.compileArguments(b, sql, objectExist, conn);
		
		// execute
		prep.execute();
		
		int id = -1;
		if (objectExist)
			id = scanner.getIdValue();
		else
			id = this.getDatabaseBackend().getIdentityValue(conn);
		
		return id;
	}

	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  DELETE
	//---------------------------------------------------------------------------------

	/***
	 * Deletes a protobuf class to database.
	 * @param b
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public void delete(MessageOrBuilder b) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		
		try {
			conn = this.initialize();
			conn.setAutoCommit(false);
			
			this.delete(b, conn);
			
			conn.commit();
		}
		catch (SQLException e) {			
			try {
				if (conn != null)
					conn.rollback();

			} catch (SQLException sqlEx) {}
			
			throw e;
		}		
		catch (Exception e) {
			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();
			
			throw e;
		}		
		finally {
			this.disconnect(conn);
		}
		


	}

	/***
	 * Internal delete function. Deletes protobuf class from database.
	 * @param b
	 * @param conn
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	private void delete(MessageOrBuilder b, Connection conn) throws SQLException, ClassNotFoundException {
		ProtoDBScanner scanner = new ProtoDBScanner(b, this.getDatabaseBackend());;
				
		// delete underlying objects
		for(FieldDescriptor field : scanner.getObjectFields()) {
			Object o = b.getField(field);

			if (field.getJavaType() == JavaType.MESSAGE && !field.isRepeated()) {
				delete((MessageOrBuilder)o, conn);
			}
		}		

		deleteBlobs(scanner, conn);
		
		// delete underlying repeated objects
		for(FieldDescriptor field : scanner.getRepeatedObjectFields()) {
			int fieldCount = b.getRepeatedFieldCount(field);
			for (int i=0;i<fieldCount;i++) {
				Object mg = b.getRepeatedField(field, i);
				if (mg instanceof MessageOrBuilder) {
					MessageOrBuilder b2 = (MessageOrBuilder)mg;
					ProtoDBScanner other = new ProtoDBScanner(b2, this.getDatabaseBackend());;
					
					// delete other object
					delete(b2, conn);
					
					// delete from link table
					deleteLinkObject(scanner, other, field, conn);
				}
			}
		}
		
		// delete underlying repeated basic types
		for (FieldDescriptor field : scanner.getRepeatedBasicFields()) {
			// delete from link table
			deleteBasicLinkObject(scanner, field, conn);			
		}
				
		//Delete the row itself
		deleteRow(scanner, conn);
	}

	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  SEARCH
	//---------------------------------------------------------------------------------

	public <T extends Message> List<T> find(T instance, String fieldName, Object searchFor, Boolean isLikeOperator) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException {
		return find(instance, fieldName, searchFor, isLikeOperator, null, -1, -1);
	}
	
	public <T extends Message> List<T> find(T instance, String fieldName, Object searchFor, Boolean isLikeOperator, List<String> excludedObjects) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException {
		return find(instance, fieldName, searchFor, isLikeOperator, excludedObjects, -1, -1);
	}
	
	

	public <T extends Message> List<T> find(T instance, String fieldName, Object searchFor, Boolean isLikeOperator, int numberOfResults, int offset) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException {
		return find(instance, fieldName, searchFor, isLikeOperator, null, numberOfResults, offset);
	}
	

	/***
	 * 
	 * @param instance			An (empty) instance of the protobuf entity to initiate search on 
	 * @param fieldName   		The field name in the object to match the search criteria
	 * 							Specify a field in a sub object by separating the objects by dot (.)
	 * 							I.e. obj1.obj2.fieldXX 
	 * @param searchFor			The search criteria
	 * @param isLikeOperator	Specifies if the search criteria contains wild characters i.e. %
	 * @param excludedObjects	List of objects paths that should be excluded from the get method
	 * 							when retrieving results
	 * @param maxResults		Maximum number of results to retrieve. Specify -1 for unlimited results.
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws SearchFieldNotFoundException
	 */
	public <T extends Message> List<T> find(T instance, String fieldName, Object searchFor, Boolean isLikeOperator, List<String> excludedObjects,  int numberOfResults, int offset) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException {
		// if field is repeated -> search link objects
		Connection conn = null;
		List<T> result = new ArrayList<T>();
		
		try {
			conn = this.initialize();
			
			result = this.find(instance, getFieldQueue(fieldName), searchFor, isLikeOperator, excludedObjects, conn, numberOfResults, offset);
			
		}
		catch (Exception e) {
			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();
			
			throw e;
		}		
		finally {
			this.disconnect(conn);
		}
		
		return result;		
	}
	
	private <T extends Message> List<T> find(T instance, Queue<String> fieldQueue, Object searchFor, Boolean isLikeFilter, List<String> excludedObjects, Connection conn, int numberOfResults, int offset) throws SearchFieldNotFoundException, SQLException, ClassNotFoundException {
		List<T> result = new ArrayList<T>();
		ProtoDBScanner scanner = new ProtoDBScanner(instance, this.getDatabaseBackend());;
		
		// Get first field
		String firstField = fieldQueue.poll();
		Logger.log(String.format("Searching for field :: %s", firstField));
		
		DBStatement prep = new DBStatement(conn);
		
		// If the size of the queue is zero this means that we only had one field in the queue
		if (fieldQueue.size() == 0) {
			Logger.log("Last field.. preparing statement");
			prep = prepareStatementSingleObject(firstField, searchFor, isLikeFilter, conn, scanner);
		}
		else {
			// object fields
			// Find the object field matching the first name in the field name path.
			Logger.log("Searching object fields");
			prep = searchObjectFields(fieldQueue, searchFor, isLikeFilter, excludedObjects, numberOfResults, offset, conn, scanner,
					firstField);
			
			// if not found search all repeated fields
			Logger.log("Searching repeated fields");
			if (prep != null && prep.getMatchingField() == null) {
				prep = searchRepeatedFields(searchFor, isLikeFilter, excludedObjects, numberOfResults, offset, conn, scanner,
						firstField, fieldQueue);
				
			}
		}

		// TODO: This could be done a lot more efficient by doing smarter sql queries in 
		// the search method above.
		if (prep != null && prep.getStatement() != null) {
			ResultSet rs = prep.executeQuery();
			result = getAllObjects(instance, numberOfResults, conn, rs, excludedObjects);
		}
		
		return result;
	}
	
	private Queue<String> getFieldQueue(String fields) {
		Queue<String> fieldQueue = new LinkedList<String>();
		
		String[] list = StringUtils.split(fields, ".");
		
		for (String s : list)
			fieldQueue.add(s);
		
		return fieldQueue;
	}

	private <T extends Message> List<T> getAllObjects(
			T instance, 
			int maxResults, 
			Connection conn, 
			ResultSet rs,
			List<String> excludedObjects) throws SQLException {
		
		List<Integer> ids = new ArrayList<Integer>();
		List<T> result = new ArrayList<T>();
		
		int counter = 0;
		while (rs.next()) {
			counter++;
			ids.add(rs.getInt(1));
			
			if (maxResults > 0 && counter >= maxResults)
				break;
		}
		
		for (int i : ids) {
			result.add(this.get(i, excludedObjects, instance, conn));
		}
		
		return result;
	}

	private DBStatement searchRepeatedFields(
			Object searchFor, 
			Boolean isLikeFilter, 
			List<String> excludedObjects,
			int numberOfResults,
			int offset, 
			Connection conn, 
			ProtoDBScanner scanner, 
			String firstField, 
			Queue<String> fieldQueue)
			throws SearchFieldNotFoundException, SQLException, ClassNotFoundException {
		
		DBStatement prep = new DBStatement(conn);
		
		// check repeated fields
		for (FieldDescriptor field : scanner.getRepeatedObjectFields()) {
			//find sub objects that match the criteria
			if (field.getName().equalsIgnoreCase(firstField)) {
				Logger.log(String.format("Found match on %s",  field.getName()));
				prep.setMatchingField(field);
				
				List<DynamicMessage> dmObjects = 
						find(
							  DynamicMessage.getDefaultInstance(field.getMessageType())
							, fieldQueue
							, searchFor
							, isLikeFilter
							, excludedObjects
							, conn
							, numberOfResults
							, offset);
				
				if (dmObjects.size() > 0) {
					List<Integer> ids = new ArrayList<Integer>();
					FieldDescriptor idField = field.getMessageType().findFieldByName("ID");
					if (idField != null) 
						for (DynamicMessage dmpart : dmObjects)
							ids.add((int)dmpart.getField(idField));
					
					//find main objects that contain the sub objects (ID-wise)
					ProtoDBScanner other = new ProtoDBScanner(dmObjects.get(0), this.getDatabaseBackend());
					prep.prepareStatement(scanner.getSearchStatementLinkObject(field, other, ids));
				}
			}
		}
		
		if (prep != null && prep.getMatchingField() == null)
			throw new SearchFieldNotFoundException(firstField, scanner.getObjectName());
		
		return prep;
	}

	private DBStatement searchObjectFields(
			Queue<String> fieldQueue, 
			Object searchFor, 
			Boolean isLikeFilter,
			List<String> excludedObjects, 
			int numberOfResults,
			int offset, 
			Connection conn, 
			ProtoDBScanner scanner, 
			String firstField) throws SearchFieldNotFoundException, SQLException, ClassNotFoundException {
		
		DBStatement prep = new DBStatement(conn);
		
		for (FieldDescriptor field : scanner.getObjectFields()) {
			if (field.getName().equalsIgnoreCase(firstField)) {
				Logger.log(String.format("Found match on %s",  field.getName()));
				prep.setMatchingField(field);
				
				List<DynamicMessage> matchingSubObjects = null;
				List<Integer> ids = new ArrayList<Integer>();
				if (field.getJavaType() == JavaType.MESSAGE) {
					// If field is a sub object then make a recursive call
					Logger.log("Making recursive call on object");
					DynamicMessage innerInstance = DynamicMessage.getDefaultInstance(field.getMessageType());
					
					matchingSubObjects = 
						find(innerInstance,
							fieldQueue,
							searchFor,
							isLikeFilter,
							excludedObjects,
							conn,
							numberOfResults,
							offset);
					
					// for each matching sub instance add the id's to a list
					FieldDescriptor idField = field.getMessageType().findFieldByName("ID");
					if (idField != null) {
						for (DynamicMessage m : matchingSubObjects)
							ids.add((int)m.getField(idField));
					}
					Logger.log(String.format("Number of IDs found :: %s", ids.size()));
					
				}
				else if (field.getJavaType() == JavaType.ENUM) {
					// if field is an enum field then make a call to then enum find function
					Logger.log("Field is an enum. Searching enum field");
					ids =
						find(field.getEnumType(),
							scanner,
							fieldQueue,
							searchFor,
							conn);
				}
				
				// get all messages of this type that have matching sub objects
				Logger.log("Preparing statement --::>" );
				
				String statement = scanner.getSearchStatementSubObject(field, ids);
				Logger.log(statement);
				
				prep.prepareStatement(statement);
			}
		}
		
		return prep;
	}

	private DBStatement prepareStatementSingleObject(
			String fieldName, 
			Object searchFor, 
			Boolean isLikeFilter, 
			Connection conn,
			ProtoDBScanner scanner) throws SearchFieldNotFoundException, SQLException {
		
		DBStatement prep = null;
		FieldDescriptor matchingField = null;
		
		for (FieldDescriptor field : scanner.getBasicFields()) {
			if (field.getName().equalsIgnoreCase(fieldName)) {
				matchingField = field;
				break;
			}
		}
		
		if (matchingField == null)
			throw new SearchFieldNotFoundException(fieldName, scanner.getObjectName());
		
		prep = new DBStatement(matchingField, scanner.getSearchStatement(matchingField, isLikeFilter), conn);
		
		Logger.log(String.format("Adding argument :: %s", searchFor));
		
		if (matchingField.getJavaType() == JavaType.BOOLEAN)
			prep.addString((Boolean)searchFor ? "Y": "N");
		else
			prep.addObject(searchFor);
		
		return prep;
	}
	

	private List<Integer> find(
			EnumDescriptor enumType, 
			ProtoDBScanner scanner,
			Queue<String> fieldQueue,
			Object searchFor, 
			Connection conn) throws SearchFieldNotFoundException, SQLException {
		
		List<Integer> ids = new ArrayList<Integer>();
		
		String fieldName = fieldQueue.poll();
		
		if (fieldQueue.size() == 0) {
			PreparedStatement prep = conn.prepareStatement(scanner.getSearchStatement(enumType));
			prep.setString(1, searchFor.toString());

			ResultSet rs = prep.executeQuery();
			
			while (rs.next()) {
				ids.add(rs.getInt(1));
			}
			rs.close();
		}
		else {
			throw new SearchFieldNotFoundException(fieldName, enumType.toString());
		}
		return ids;
	}

	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, fieldName, searchFor, op, false, null, -1, -1);
	}

	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op, boolean searchShallow) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, fieldName, searchFor, op, searchShallow, null, -1, -1);
	}
	
	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op, int numberOfResults, int offset) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, fieldName, searchFor, op, false, null, numberOfResults, offset);
	}
	
	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op, boolean searchShallow, List<String> excludedObjects) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, fieldName, searchFor, op, searchShallow, excludedObjects, -1, -1);
	}
	
	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op, boolean searchShallow, int numberOfResults, int offset) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		return search(instance, fieldName, searchFor, op, searchShallow, null, numberOfResults, offset);
	}
	
	public <T extends Message> List<T> search(T instance, String fieldName, Object searchFor, ProtoDBSearchOperator op, boolean searchShallow, List<String> excludedObjects,  int numberOfResults, int offset) throws ClassNotFoundException, SQLException, SearchFieldNotFoundException, ProtoDBParserException {
		Connection conn = null;
		
		try {
			conn = this.initialize();

			ProtoDBScanner scanner = new ProtoDBScanner(instance, this.getDatabaseBackend());
			JoinResult joinClause = Searcher.getJoinQuery(scanner, populateBlobs, !searchShallow, numberOfResults, offset);
			
			// check if this is a repeated (or enum)
			joinClause.addWhereClause(scanner, fieldName, searchFor, op);
			
			PreparedStatement prep = joinClause.getStatement(conn);
			
			ResultSet rs = prep.executeQuery();

			// the search might as well be a joined query over repeated objects
			// if no many-many joins are made then we can get the results directly
			// otherwise we need to do subqueries on the individual objects.
			// since we are calling on the parent the subqueries should return all
			// subobjects regardless of the search criteria (maybe this could be
			// set as a parameter)
			List<T> result = joinClause.getResult(instance, rs, this.isPopulateBlobsActive());
			
			if (joinClause.hasComplexJoins())
				result = getByJoin(result, false);

			return result;
		}
		catch (Exception e) {
			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();
			
			throw e;
		}		
		finally {
			this.disconnect(conn);
		}		
	}
	
	//---------------------------------------------------------------------------------
	//----------------------------------------------------------------------  HELPERS
	//---------------------------------------------------------------------------------

	
	/***
	 * Internal function to check if table exists
	 * @param tableName
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	private boolean tableExist(String tableName, Connection conn) throws SQLException {
		return this.getDatabaseBackend().tableExist(tableName, conn);
	}
	
	public void dropAllTables() throws SQLException, ClassNotFoundException {
		Connection conn = null;

		try {
			conn = this.initialize();
			
			List<String> tables = this.getDatabaseBackend().getAllTables(conn);
			
			for (String t : tables) {
				PreparedStatement prep = 
					conn.prepareStatement(
						String.format("DROP TABLE %s%s%s", 
							this.getDatabaseBackend().getStartBracket(),
							t,
							this.getDatabaseBackend().getEndBracket()));
				
				prep.execute();				
			}
		}
		catch (SQLException | ClassNotFoundException e) {			
			try {
				if (conn != null)
					conn.rollback();

			} catch (SQLException sqlEx) {}

			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();

			throw e;
		}		
		finally {
			this.disconnect(conn);
		}
	}
	
	public void executeNonQuery(String sql) throws Exception {
		Connection conn = null;

		try {
			conn = this.initialize();
			PreparedStatement prep = conn.prepareStatement(sql);
			prep.execute();
		}
		catch (SQLException | ClassNotFoundException e) {			
			try {
				if (conn != null)
					conn.rollback();

			} catch (SQLException sqlEx) {}

			System.out.println("Exception in ProtoDB!");
			e.printStackTrace();

			throw e;
		}		
		finally {
			this.disconnect(conn);
		}
	}
	
	public DBType getDBType() {
		return this.getDatabaseBackend().getDBType();
	}
	
}

package main.java.api;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;



/**
 * 
 *<p>
 * AmazonRDS class has implementation for the main Functions: </p>
 * <p><i>
 * Connect, Query, getProperty,getEntity, put, deleteEntity</p></i>
 * 
 * @CDport 1.0
 * 
 */

public class AmazonRDS {

	private static AmazonRDSClient rds;
	private 	Connection conn = null;

	/**
	 *Constructor.
	 * 
	 * @param
	 */
	public AmazonRDS(){
		super();
	}

	/**
	 * Connect to AmazonRDS.
	 * 
	 *  @param
	 *  
	 */
	
	public void connectToAmazonRDS(String accessKey, String secretKey, String regionName, String endPoint, String jdbcUrl ) {
		//--set the credentials--
		 AWSCredentials credentials  = new BasicAWSCredentials(accessKey, secretKey);
	     rds = new AmazonRDSClient(credentials);
	     //--set the region--
	     Region r=null;
	     if(regionName.equalsIgnoreCase("US_EAST_1"))
		 r = Region.getRegion(Regions.US_EAST_1);
	     else if(regionName.equalsIgnoreCase("US_WEST_1"))
		 r = Region.getRegion(Regions.US_WEST_1);
	     else if(regionName.equalsIgnoreCase("US_WEST_2"))
			 r = Region.getRegion(Regions.US_WEST_2);
	     else if(regionName.equalsIgnoreCase("EU_WEST_1"))
			 r = Region.getRegion(Regions.EU_WEST_1);
	     else if(regionName.equalsIgnoreCase("AP_SOUTHEAST_1"))
			 r = Region.getRegion(Regions.AP_SOUTHEAST_1);
	     else if(regionName.equalsIgnoreCase("AP_SOUTHEAST_2"))
			 r = Region.getRegion(Regions.AP_SOUTHEAST_2);
	     else if(regionName.equalsIgnoreCase("AP_NORTHEAST_1"))
			 r = Region.getRegion(Regions.AP_NORTHEAST_1);
	     else if(regionName.equalsIgnoreCase("SA_EAST_1"))
	    r = Region.getRegion(Regions.SA_EAST_1);
	     
		rds.setRegion(r);
		rds.setEndpoint(endPoint);
		
	    System.out.println("===========================================");
	    System.out.println("Getting Started with AmazonRDS");
	    System.out.println("===========================================\n");
	    
//___________________________________________________________________________
//___________________________________________________________________________

	    try {
            // The newInstance() call is a work around for some//___________________________________________________________________________

            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
        }
	DescribeDBInstancesResult dbr= rds.describeDBInstances();
	

	try {
	    conn =
	       DriverManager.getConnection(jdbcUrl);

	    System.out.println("===========================================");
	    System.out.println("Connection"+dbr.getDBInstances().get(0).getEndpoint().getAddress());
	    System.out.println("===========================================\n");

	    //connectionTest( );
	    
	} catch (SQLException ex) {
	    // handle any errors
	    System.out.println("SQLException: " + ex.getMessage());
	    System.out.println("SQLState: " + ex.getSQLState());
	    System.out.println("VendorError: " + ex.getErrorCode());
	}
	//--------------------------------------------------------------------------------
	
	}
	
	
	
	public  List<String> getPrimaryKeyName(String entityType){///----------------get primary key name ------------------
	   List<String> primarykey =new ArrayList< String>() ;
	 try{
		DatabaseMetaData databaseMetaData = conn.getMetaData();
    ResultSet result = databaseMetaData.getPrimaryKeys(null, null, entityType);
	    while(result.next()){
	    	primarykey.add(result.getString(4));
	       //System.out.println("primarykey:"+ primarykey);// get the name  of the primary key..
	    }
   }catch (SQLException ex) {
	    // handle any errors
	    System.out.println("SQLException: " + ex.getMessage());
	    System.out.println("SQLState: " + ex.getSQLState());
	    System.out.println("VendorError: " + ex.getErrorCode());
	}
    return primarykey;
	}
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute Queries</p>
	 *
	 * @param QueryStatement the string query statement. 
	 * @param
	 * 
	 * @return  Response 
	 */
	public Response query(String QueryStatement, String entityType) {
		//*_Before execute query, check  entityType exist.!!
		/*
	if(listofEntityTypes().contains(entityType)||listofCatalogs().contains(entityType))
	{		
*/ System.out.println("Query...."+QueryStatement);
		Response rsp=new Response();
		 Statement statement=null;
		try{
		//if(listofEntityTypes().contains(entityType)){
			if(QueryStatement.contains("SELECT")||QueryStatement.contains("select")){
		List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........
		    //-------------------------------------------------------------
		       statement = conn.createStatement();
		      // resultSet gets the result of the SQL query
		     ResultSet  resultSet = statement.executeQuery(QueryStatement);

		   //------
			   List<Entities> entityList =new ArrayList<Entities>();
			    
				//List<Properties> propList=new ArrayList<Properties>();

			     ResultSetMetaData rsmd = resultSet.getMetaData();
			    
			    // List<String> col=new ArrayList<String>();
			  while (resultSet.next()) 
			  {
				  Entities entity= new Entities();
			    EntityKey key=new EntityKey();
				//--------------set entity type
			    entity.setEntityType(entityType);
			    
			    List<Long> keysgroup=new   ArrayList<Long> ();
			     int x=1;
			 //---------properties-------------
			 while (x<=rsmd.getColumnCount())
			    {//col.add( rsmd.getColumnName(x));
				    String attrName=rsmd.getColumnName(x);
				    String colType=rsmd.getColumnTypeName(x);
					 PropertyValue attrValue=new PropertyValue();
					//..
					    //--------------set entity key
						if(primarykey.size()>1 )
						 {int j=0;
							 while (j<primarykey.size()){
								if( attrName.equalsIgnoreCase(primarykey.get(j)))
								//check type of key
							
								// if (colType.equalsIgnoreCase("VARCHAR")||colType.equalsIgnoreCase("CHAR"))
								     if (colType.equalsIgnoreCase("INT"))
								    	 keysgroup.add(Long.valueOf( resultSet.getInt(attrName) ));
								    else	 if (colType.equalsIgnoreCase("long"))
								    	keysgroup.add(resultSet.getLong(attrName) );
								    else  keysgroup.add(Long.valueOf( resultSet.getString(attrName) ));


							
								j++;}						
							}
						//..
					 //--------------set entity key
					if(primarykey.size()==1 && attrName.equalsIgnoreCase(primarykey.get(0)))
						 {//check type of key
							// if (colType.equalsIgnoreCase("VARCHAR")||colType.equalsIgnoreCase("CHAR"))
							     if (colType.equalsIgnoreCase("INT"))
							    	key.setIntkey( resultSet.getInt(attrName) );
							    else	 if (colType.equalsIgnoreCase("long"))
							    	key.setIntkey( resultSet.getLong(attrName) );
							    else  key.setStringkey(resultSet.getString(attrName) );
						
						entity.setKey(key);
						 }
					else{//add col. to property list
					//-----------check property data type    
				    if (colType.equalsIgnoreCase("VARCHAR")||colType.equalsIgnoreCase("CHAR"))
				    	attrValue.setString(resultSet.getString(attrName) );
				    else	 if (colType.equalsIgnoreCase("INT")||colType.equalsIgnoreCase("smallint"))
				    	attrValue.setLong( resultSet.getInt(attrName) );
				    else	 if (colType.equalsIgnoreCase("long"))
				    	attrValue.setLong( resultSet.getLong(attrName) );
				    else	 if (colType.equalsIgnoreCase("boolean"))
				    	attrValue.setBoolean( resultSet.getBoolean(attrName) );
				    else	 if (colType.equalsIgnoreCase("date")||colType.equalsIgnoreCase("datetime"))
				    	attrValue.setDate( resultSet.getDate(attrName) );
				    else	 if (colType.equalsIgnoreCase("double"))
				    	attrValue.setDouble( resultSet.getDouble(attrName) );
				    else	 if (colType.equalsIgnoreCase("float"))
				    {double d=resultSet.getFloat(attrName);
				    	attrValue.setDouble( d );}
				    else	 if (colType.equalsIgnoreCase("DECIMAL"))
				    {double d=resultSet.getBigDecimal(attrName).doubleValue();
				    	attrValue.setDouble( d );}
				    else 
				    	attrValue.setString(resultSet.getString(attrName) );//*********
				    
				   //-------add property to entity
				    Properties property= new Properties();
					property.setProperity(attrName, attrValue);
					entity.setProperties(property);
			    } 
				x++;
			 }
			 if(primarykey.size()>1) {key.setListkey(keysgroup); entity.setKey(key); }
				 //add entity to the list of entities
			     entityList.add(entity);
				}
					rsp.setEntities(entityList); 
			    
					    System.out.println("....select executed.... ");
			}
			else//insert..update..delete..drop
			{statement = conn.createStatement();
		     statement.executeUpdate(QueryStatement);
		     System.out.println(".... executed.... ");
			}
	//	}
		//......if table not found in entity list.......
		//....check is the query is it  create !!.......
		//else
			if(QueryStatement.contains("create")||QueryStatement.contains("CREATE"))
		{	statement = conn.createStatement();
			statement.executeUpdate(QueryStatement);
	       }
		//else {System.out.println("Can't execute query:"+QueryStatement+".. Entity Type not exist..."); }

		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	

  	  return rsp;	
	}
		/**
		 * <p> Get Property.</p>
		 * <p>
		 * Get all properties that belong to the <i> entity type </i>and the <i>Entity Key</i> that are specified.</p>
		 * EntityKey can be <b> String </b> only.
		 * 
		 * @return Response
		 */
		
		
	public Response getProperty(String entityType,EntityKey entityKey) {
	
		Response rp=new Response();

		
		try{
	//*_Before execute query, check  entityType exist.!!
	if(listofEntityTypes().contains(entityType)){
		String sql= "select * from "+entityType+" where ";
		///----------------get primary key name ------------------
		List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........
		//..
		
	    //--------------set entity key
		if(primarykey.size()>1 )
		 {int j=0;
			 while (j<primarykey.size()){		
				  //----------query statement-------------
					sql+= primarykey.get(j) +"="+entityKey.getListkey().get(j);
				j++;
				if(j<primarykey.size() ) sql+=" and ";
			 	}
			 
			}
		//..
	 //--------------set entity key
	if(primarykey.size()==1 )
		 {//check type of key
	    //----------query statement-------------
	sql+= primarykey.get(0) +" ="+entityKey.getIntkey();
	}
		System.out.println(sql);
		
	    Statement statement = conn.createStatement();
	      // resultSet gets the result of the SQL query
	     ResultSet  resultSet = statement.executeQuery(sql);

//------

			List<Properties> propList=new ArrayList<Properties>();

		     ResultSetMetaData rsmd = resultSet.getMetaData();
		    
		     int x=1;
		    // List<String> col=new ArrayList<String>();
		  while (resultSet.next()) 
		  {
		     while (x<=rsmd.getColumnCount())
			    {	Properties p=new Properties();
				    PropertyValue attrValue= new PropertyValue();
				 //col.add( rsmd.getColumnName(x));
			    String attrName=rsmd.getColumnName(x);
			    String colType=rsmd.getColumnTypeName(x);
			   
			    //--------------set entity key
				// if(attrName.equalsIgnoreCase(primarykey))
				//	 ;
				// else{//if it is not key--add it to property list
				  
					//-----------check property data type    
					    if (colType.equalsIgnoreCase("VARCHAR")||colType.equalsIgnoreCase("CHAR"))
					    	attrValue.setString(resultSet.getString(attrName) );
					    else	 if (colType.equalsIgnoreCase("INT")||colType.equalsIgnoreCase("smallint"))
					    	attrValue.setLong((long) resultSet.getInt(attrName) );
					    else	 if (colType.equalsIgnoreCase("long"))
					    	attrValue.setLong( resultSet.getLong(attrName) );
					    else	 if (colType.equalsIgnoreCase("boolean"))
					    	attrValue.setBoolean( resultSet.getBoolean(attrName) );
					    else	 if (colType.equalsIgnoreCase("date")||colType.equalsIgnoreCase("datetime"))
					    	attrValue.setDate( resultSet.getDate(attrName) );
					    else	 if (colType.equalsIgnoreCase("double"))
					    	attrValue.setDouble( resultSet.getDouble(attrName) );
					    else	 if (colType.equalsIgnoreCase("float"))
					    {double d=resultSet.getFloat(attrName);
					    	attrValue.setDouble( d );}
					    else	 if (colType.equalsIgnoreCase("DECIMAL"))
					    {double d=resultSet.getBigDecimal(attrName).doubleValue();
					    	attrValue.setDouble( d );}
					    else 
					    	attrValue.setString(resultSet.getString(attrName) );//*********
			    
			 //   System.out.println(x+">"+attrName+"_colType_"+colType);
			    p.setProperity(attrName, attrValue);
			    propList.add(p);
				// }
		  x++;
		   } 
		    }
			rp.setProperties(propList);

		  System.out.println(".... executed.... ");

			 
	}else System.out.println("Can't get_Entity.. Entity Type not exist...");
			
		}catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	return rp;
	}
		
	/**
	 * <p> Get Entity.</p>
	 * <p>
	 * Get all Entities that belong to the specified <i> Entity type </i>.</p>
	 * EntityKey can be a <b> String </b> only.
	 * 
	 * @return Response
	 * 
	 */
		
	public Response getEntity(String entityType) {
		String sql= "select * from "+entityType;

		Response rsp=new Response();
		
		if(listofEntityTypes().contains(entityType))
		 rsp= query(sql,entityType);
		else System.out.println("Can't get_Entity.. Entity Type not exist...");
		
		
		return rsp;
		}
	//....
	public Response getEntity(String entityType, int from, int to) {
		String sql= "select * from "+entityType+" limit "+to+" offset "+from;

		Response rsp=new Response();
		//if(listofEntityTypes().contains(entityType))
		 rsp= query(sql,entityType);
		//else System.out.println("Can't get_Entity.. Entity Type not exist...");
		return rsp;
	}//.....
	public Response getEntity(String entityType, List<EntityKey> keys) {
		String sql= "select * from "+entityType+ " where ";
		List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........
		
		 //--------------set entity key
		int j=0;
	do{
		sql+=primarykey.get(j);
		sql+= " IN ( ";
		
		for (int i=0;i<keys.size();i++)
			{		

			String key="";
			//add key ..check type
			if(keys.get(i).hasIntkey())  key=keys.get(i).getIntkey()+"";
			else if(keys.get(i).hasObjectIdkey()) key="'"+keys.get(i).getObjectIdkey().toString()+"'"; //***********
			else if(keys.get(i).haslistkey()) key=keys.get(i).getListkey().get(j)+""; //***********
			else key="'"+keys.get(i).getStringkey()+"'"; //***********
			sql+=key;
			if(i<keys.size()-1) sql+=" , ";
			}
		sql+=" ) ";
		
		
		j++;
		if(primarykey.size()<1 )break;
		else if(j<primarykey.size()) sql+= " and ";
	} while (j<primarykey.size());
								
		
		
System.out.println("select "+sql);
		Response rsp=new Response();
		if(listofEntityTypes().contains(entityType))
		 rsp= query(sql,entityType);
		else System.out.println("Can't get_Entity.. Entity Type not exist...");
		
		
		return rsp;
		}
	/**
	 * <p> Put.</p>
	 * <p>
	 * To add new Entity. The method also supports replace entity if exist. </p>
	 * 
	 * @param entityType String. (It is the 'domain' name in SimpleDB).
	 * @param entityKey (It can be <b> String<b/> only)
	 * @param propertyList The list of properties of the new entity.The property value can be a <b>String or an array of String</b>.
	 * @param ReplaceIfExist (boolean variable). Set it to <b> true </b> if you want to replace the entity if it exists. 
	 *   	Then, the entire entity properties will be replaces by the new entity properties. 
	 *   	If it <b> false </b> and the entity key is exist, so no changes will be performed even if the properties list is different.
	 *
	 */
	public void put(String entityType,EntityKey entityKey,List<Properties> propertyList,boolean ReplaceIfExist ) {
		Statement stmt=null;
		try{
			stmt = conn.createStatement();
		
	  //======================check if table not exist================================
	    if(listofEntityTypes().contains(entityType)==false){
			//create table
			String createSql="CREATE TABLE "+entityType +" ( id"; //column_name1 data_type(size),column_name2 data_type(size),
			//add key ..check type
			if(entityKey.hasIntkey()) createSql+="  bigint  NOT NULL";
			else createSql+=" VARCHAR (255) NOT NULL";    
			// coloumn
				int x=0;
				while(x<propertyList.size())
				{ createSql+=" , ";
				createSql+=propertyList.get(x).getPropertyName();
				//check type of coloum
				PropertyValue value=propertyList.get(x).getPropertyValue();
						if(value.haslongValue())
							createSql+=" int (255) ";
						else if(value.hasDoubleValue())
							createSql+=" DOUBLE ";
						else if (value.hasDateValue())
							createSql+=" DATE ";
						else if (value.hasbooleanValue())
							createSql+=" BOOLEAN ";
						else if(value.hasStringValue())
							createSql+=" VARCHAR (255) ";


					x++;
				}createSql+=", PRIMARY KEY (id) )";
				System.out.println("createSql>"+createSql);
			      stmt.executeUpdate(createSql);

		}
	   //========================================================================================== 
	  ///----------------get primary key name ------------------
		List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........
	    //------Query statment-------
	    String sql="";
	    String sqlUpdate="";

	    if(primarykey.isEmpty()) sql="INSERT INTO "+entityType +" ( id";
	    else sql="INSERT INTO "+entityType +" ( "+primarykey.get(0); // (column1,column2,column3,...) VALUES (value1,value2,value3,...)" ;
	  //  else //update
	    	sqlUpdate="UPDATE "+entityType +" SET  ";//+primarykey.get(0) +"="; 
	    // coloumn
		int x=0;
		while(x<propertyList.size())
		{ sql+=" , ";
			sql+=propertyList.get(x).getPropertyName();
			x++;
			
		} sql+=")";
		//values
		sql+=" VALUES ( ";
		int y=0;
		String key="";
		//add key ..check type
		if(entityKey.hasIntkey())  key=entityKey.getIntkey()+"";
		else if(entityKey.hasObjectIdkey()) key="'"+entityKey.getObjectIdkey().toString()+"'"; //***********
		else key="'"+entityKey.getStringkey()+"'"; //***********
		
		sql+=key;
		
		//-----------------------------
		while(y<propertyList.size())
		{ sql+=" , "; 
		//CHECK TYPE
			PropertyValue value=propertyList.get(y).getPropertyValue();
			if(value.hasArrayValue())
			{sql+=propertyList.get(y).getPropertyValue().getArray();//***************************************check array type
			sqlUpdate+=propertyList.get(y).getPropertyName()+"="+propertyList.get(y).getPropertyValue().getArray();
			}
			else if(value.haslongValue())
			{sql+=propertyList.get(y).getPropertyValue().getLong();
			sqlUpdate+=propertyList.get(y).getPropertyName()+"="+propertyList.get(y).getPropertyValue().getLong();
			}
			else if(value.hasDoubleValue())
				{sql+=propertyList.get(y).getPropertyValue().getDouble();
				sqlUpdate+=propertyList.get(y).getPropertyName()+"="+propertyList.get(y).getPropertyValue().getDouble();
				}
			else if(value.hasDateValue())
				{sql+=propertyList.get(y).getPropertyValue().getDate();
				sqlUpdate+=propertyList.get(y).getPropertyName()+"="+propertyList.get(y).getPropertyValue().getDate();
				}
			else if(value.hasbooleanValue())
				{sql+=propertyList.get(y).getPropertyValue().getBoolean();
				sqlUpdate+=propertyList.get(y).getPropertyName()+"="+propertyList.get(y).getPropertyValue().getBoolean();
				}
			else 
				{	//*************************************** add'' automatic
				String s=propertyList.get(y).getPropertyValue().getString();
				if( s.startsWith("'")&& s.endsWith("'"))
				{sql+=s;
				sqlUpdate+=propertyList.get(y).getPropertyName()+"="+s;
				}
				else {sql+="'"+s+"'";
					sqlUpdate+=propertyList.get(y).getPropertyName()+"= '"+s+"'";
					}
				}
			
				
			y++;
			if(y<propertyList.size()) sqlUpdate+=" , ";
		}sql+=")";
	 if(ReplaceIfExist)//update
		{// is the entity exist
		ResultSet s= stmt.executeQuery("SELECT * FROM "+ entityType+" WHERE "+ primarykey.get(0)+"=" +key);
		 if(s.next())
		 {sqlUpdate+=" where "+primarykey.get(0)+" ="+ key;//******
		 System.out.println("update: "+sqlUpdate);
	      stmt.executeUpdate(sqlUpdate);
		 } 
		 else{System.out.println("insert: "+sql);
	      stmt.executeUpdate(sql);}
		}
	      else{System.out.println("insert: "+sql);
	      stmt.executeUpdate(sql);
		}

		
		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	
	public void put(String entityType,List<Entities> entities) {
		Statement stmt=null;
		try{
			stmt = conn.createStatement();
	  ///----------------get primary key name ------------------
		List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........
	    //------Query statment-------
	    String sql="";

		 sql="INSERT INTO "+entityType +" ( "+primarykey.get(0); // (column1,column2,column3,...) VALUES (value1,value2,value3,...)" ;
	    // coloumn
		List< Properties> propertyNamesList=entities.get(0).getProperties();
		int x=0;
		while(x<propertyNamesList.size())
		{ sql+=" , ";
			sql+=propertyNamesList.get(x).getPropertyName();
			x++;
		} sql+=")";
		//values
		sql+=" VALUES  ";
		//....................
		
	for(int EntityIndex=0;EntityIndex< entities.size();EntityIndex++){
		EntityKey entityKey=entities.get(EntityIndex).getKey();
		List< Properties> propertyList=entities.get(EntityIndex).getProperties();
		int y=0;
		String key="";
		//add key ..check type
		if(entityKey.hasIntkey())  key=entityKey.getIntkey()+"";
		else if(entityKey.hasObjectIdkey()) key="'"+entityKey.getObjectIdkey().toString()+"'"; //***********
		else key="'"+entityKey.getStringkey()+"'"; //***********
		
		sql+="( "+key;		
		//-----------------------------
		while(y<propertyList.size())
		{ sql+=" , "; //sqlUpdate+=" , ";
		//CHECK TYPE
			PropertyValue value=propertyList.get(y).getPropertyValue();
			if(value.hasArrayValue())
			{sql+=propertyList.get(y).getPropertyValue().getArray();//***************************************check array type
			}
			else if(value.haslongValue())
			{sql+=propertyList.get(y).getPropertyValue().getLong();
			}
			else if(value.hasDoubleValue())
				{sql+=propertyList.get(y).getPropertyValue().getDouble();
				}
			else if(value.hasDateValue())
				{sql+=propertyList.get(y).getPropertyValue().getDate();
				}
			else if(value.hasbooleanValue())
				{sql+=propertyList.get(y).getPropertyValue().getBoolean();
				}
			else 
				{	//*************************************** add'' automatic
				String s=propertyList.get(y).getPropertyValue().getString();
				if( s.startsWith("'")&& s.endsWith("'"))
				{sql+=s;
				}
				else {sql+="'"+s+"'";
					}
				}
			
				
			y++;
		}sql+=")";
		if(EntityIndex< entities.size()-1) sql+=" , ";
	}//end for
	 //System.out.println("insert: "+sql);
	      stmt.executeUpdate(sql);
		
		
		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ) {
			//*_check if key is exist
		String key="";
		//add key ..check type
		if(entityKey.hasIntkey())  key=entityKey.getIntkey()+"";
		else if(entityKey.hasObjectIdkey()) key="'"+entityKey.getObjectIdkey().toString()+"'"; //***********
		else key="'"+entityKey.getStringkey()+"'"; //***********
		
		//*_Before delete, check if type exist 
				if(listofEntityTypes().contains(entityType)){
						Statement stmt=null;
						try{
							List<String> primarykey=	getPrimaryKeyName(entityType);//***get entity key ***..........	
							stmt = conn.createStatement();
							stmt.executeUpdate("delete from "+entityType+" where "+primarykey.get(0)+"="+key);
				System.out.println("Delete Entity Type called " + entityType + ".\n");
				
					} catch (SQLException ex) {
					    // handle any errors
					    System.out.println("SQLException: " + ex.getMessage());
					    System.out.println("SQLState: " + ex.getSQLState());
					    System.out.println("VendorError: " + ex.getErrorCode());
					}
				}else System.out.println(" Entity Type  " + entityType + " No exist");
	}
	/**
	 * <p>
	 * Delete EntityType. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		//*_Before delete, check if type exist 
		if(listofEntityTypes().contains(entityType)){
				Statement stmt=null;
				try{
					stmt = conn.createStatement();
					stmt.executeUpdate("DROP TABLE "+entityType);
		System.out.println("Delete Entity Type called " + entityType + ".\n");
			} catch (SQLException ex) {
			    // handle any errors
			    System.out.println("SQLException: " + ex.getMessage());
			    System.out.println("SQLState: " + ex.getSQLState());
			    System.out.println("VendorError: " + ex.getErrorCode());
			}
		}else System.out.println(" Entity Type  " + entityType + " No exist");
	}
	/**
	 * <p>
	 * Create Entity Type. </p>
	 * 
	 * 
	 */
	public void createEntityType(String entityType){
		   
		// Create a Entity Type
		    System.out.println("Creating Entity Type called " + entityType + ".\n");
		  //-------------------------------------------------------------
		      Statement statement;
			try {
				statement = conn.createStatement();
		      // resultSet gets the result of the SQL query
		     statement.executeQuery("create table "+entityType);

		   //------
			} catch (SQLException ex) {
			    // handle any errors
			    System.out.println("SQLException: " + ex.getMessage());
			    System.out.println("SQLState: " + ex.getSQLState());
			    System.out.println("VendorError: " + ex.getErrorCode());
			}
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		List<String> list=new ArrayList<String>();
		
		//-------------------------------------------------------------
	      Statement statement;
		try {
			statement = conn.createStatement();
	      // resultSet gets the result of the SQL query
	     ResultSet  resultSet = statement.executeQuery("show tables");
	     
	     while(resultSet.next()){
	    	 String table=resultSet.getString(1);
	    	 list.add( table );

	     }
	   //------
		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}


	return list;
	}

	/**
	 * <p>
	 * List of Entity Types_Catalog. </p>
	 * 
	 */
	public List<String> listofCatalogs(){
		List<String> catalgList= new ArrayList<String>();
	return catalgList;
	}
	
	

}

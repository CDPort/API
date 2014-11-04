package main.java.api;


import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;


import com.mongodb.*;


/**
 * 
 *<p>
 * MongoDB class has implementation for the main Functions: </p>
 * <p><i>
 * Connect, Query, getProperty,getEntity, put, deleteEntity</p></i>
 * 
 * @CDport 1.0
 * 
 */
public class MongoDB {
private    DB db;
   

/**
 * Connect to MongoDB.
 * 
 *  @param
 *  
 */

	public void connectToMongoDB(String MongoClientURI) {
		db=null;
		try{ MongoClientURI uri  = new MongoClientURI(MongoClientURI); 
	        MongoClient client = new MongoClient(uri);
	         db = client.getDB(uri.getDatabase());// databaseName
	         System.out.println("===========================================");
	 	    System.out.println("Getting Started with  MongoDB");
	 	    System.out.println("===========================================\n");
		 }catch (UnknownHostException e){;}
		
 
}
	
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple select Queries like (select * from type)</p>
	 * The query statement will be converted to get the variables that are needed to execute the query by using custom APIs' functions.
	 *
	 * @param entityType string( It is the 'Collection' name). 
	 * @param condition BasicDBObject
	 * @param fields BasicDBObject (the fields that user wants to retrieve them).
	 * 
	 * @return  Response 
	 */
	
	public Response query(String entityType,BasicDBObject condition, BasicDBObject fields) {			
		Response queryResponse=new Response();
		//*_Before execute query, check  entityType exist.!!
	//if(listofEntityTypes().contains(entityType)){
	    DBCollection collection = db.getCollection(entityType);
		DBCursor cursor=null;
		// find the entities that match the condition and specified fields.
		cursor = collection.find(condition,fields);//(It  works Even if there is condition and fields=null)
		 
		//--------check if no result-----------------------
		 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);}
		 // call the 'setEntityList' method to  set all retrieved entities in a list
		 List<Entities>   entList= setEntityList( cursor,  entityType);
		 queryResponse.setEntities(entList);// set the list to the response object, and then return it.
	//}		else System.out.println("Can't execute query..Entity Type not exist...");

		return queryResponse;	
	}
	
			
	/**
	 * <p> Get Property.</p>
	 * <p>
	 * Get all properties that belong to the <i> entity type </i>and the <i>Entity Key</i> that are specified.</p>
	 * EntityKey can be <b>long</b> , <b> String <b/> or <b> ObjectID </b>.
	 * 
	 * @return Response
	 */
	
	public Response getProperty(String entityType,EntityKey entityKey) {
		Response resp=new Response();
		//*_Before execute query, check  entityType exist.!!
	if(listofEntityTypes().contains(entityType))
	{		 DBCollection coll = db.getCollection(entityType);
        BasicDBObject IdToInsert = new BasicDBObject();
        if(entityKey.hasObjectIdkey())       IdToInsert.put("_id", entityKey.getObjectIdkey()); 
        else if( entityKey.hasIntkey())      IdToInsert.put("_id", entityKey.getIntkey()); 
        else if( entityKey.hasStringkey())   IdToInsert.put("_id", entityKey.getStringkey());
	
        DBCursor cursor = coll.find(IdToInsert);
      //--------check if no result-----------------------
		 if (cursor.count()==0){System.out.print("No result match the query "); System.exit(1);;}
		
     List<Entities>   entList= setEntityList( cursor,  entityType);
     List<Properties>  propList= entList.get(0).getProperties();
     resp.setProperties(propList);;
	}		else System.out.println("Can't get_Properties..Entity Type not exist...");

		return resp;
}
	
	/**
	 * <p> Get Entity.</p>
	 * <p>
	 * Get all Entities that belong to the specified <i> Entity type </i>.</p>
	 * EntityKey can be <b>long</b> , <b> String <b/> or <b> ObjectID </b>.
	 * 
	 * @return Response
	 * 
	 */
	public Response getEntity(String entityType) {
		Response rsp=new Response();
		//*_Before execute query, check  entityType exist.!!
		if(listofEntityTypes().contains(entityType))
		{
	List<Entities> entList=new ArrayList<Entities> ();
	
		  DBCollection coll = db.getCollection(entityType);
	         DBCursor cursor = coll.find();
	       //--------check if no result-----------------------
			 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);;}

	         entList= setEntityList( cursor,  entityType);
		rsp.setEntities(entList);
		}else System.out.println("Can't get_Entity.. Entity Type not exist...");

	return rsp;
	}
	//....
		public Response getEntity(String entityType, int from, int to) {
			Response rsp=new Response();
			//*_Before execute query, check  entityType exist.!!
			if(listofEntityTypes().contains(entityType))
			{
		List<Entities> entList=new ArrayList<Entities> ();
		
			  DBCollection coll = db.getCollection(entityType);
		         DBCursor cursor = coll.find().skip(from).limit(to);
		       //--------check if no result-----------------------
				 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);;}

		         entList= setEntityList( cursor,  entityType);
			rsp.setEntities(entList);
			}else System.out.println("Can't get_Entity.. Entity Type not exist...");

		return rsp;
		}
		//.....
	public Response getEntity(String entityType, List<EntityKey>keys) {
		Response rsp=new Response();
		//*_Before execute query, check  entityType exist.!!
		if(listofEntityTypes().contains(entityType))
		{
	List<Entities> entList=new ArrayList<Entities> ();
	
		  DBCollection coll = db.getCollection(entityType);
		  
	        BasicDBObject IdToInsert = new BasicDBObject();

	        IdToInsert.put("_id", "entity1");
	        IdToInsert.append("_id", "entity4");
	        IdToInsert.append("_id", "entity8");

	        
	        DBObject clause1 = new BasicDBObject("_id", "entity1");
	        DBObject clause2 = new BasicDBObject("_id", "entity4");    
	        BasicDBList in = new BasicDBList();
	        in.add(clause1);
	        in.add(clause2);
	        DBObject query = new BasicDBObject("$in", in);
	        
	        BasicDBObject  searchObject = new BasicDBObject();
	        List keysList = new ArrayList();
	        for (int keyIndex=0;keyIndex<keys.size();keyIndex++)
	        {
	        	EntityKey k=keys.get(keyIndex);
	        	if(k.hasIntkey())
	        	keysList.add(k.getIntkey());
	        	else if(k.hasObjectIdkey())
		        	keysList.add(k.getObjectIdkey());
	        	else if(k.haslistkey())
		        	keysList.add(k.getListkey());///>>>>>>>>>>>
	        	else keysList.add(k.getStringkey());
	        }
	        searchObject.put("_id", new BasicDBObject("$in",keysList));
	        
	         DBCursor cursor = coll.find(searchObject);
	       //--------check if no result-----------------------
			 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);;}

	         entList= setEntityList( cursor,  entityType);
		rsp.setEntities(entList);
		}else System.out.println("Can't get_Entity.. Entity Type not exist...");

	return rsp;
	}

	/**
	 * <p> Put.</p>
	 * <p>
	 * To add new Entity. The method also supports replace entity if exist. </p>
	 * 
	 * @param entityType String. (It is the 'Collection' name in MongoDB).
	 * @param entityKey (It can be <b>long</b> , <b> String</b> or <b> ObjectID</b>)
	 * @param propertyList The list of properties of the new entity.The property value can be a <b>String, long, Date or an array of any of these types</b>.
	 * @param ReplaceIfExist (boolean variable). Set it to <b> true </b> if you want to replace the entity if it exists. 
	 *   	Then, the entire entity properties will be replaces by the new entity properties. 
	 *   	If it <b> false </b> and the entity key is exist, so no changes will be performed even if the properties list is different.
	 *
	 */
	public void put(String entityType,EntityKey entityKey,List<Properties> propertyList, boolean ReplaceIfExist ) {
	
		 DBCollection coll = db.getCollection(entityType)	;        
	       // ObjectId id = new ObjectId();
	        BasicDBObject IdToInsert = new BasicDBObject();
	        DBObject dbObject =null;
	    	//****************Check data type*************
	        if(entityKey.hasObjectIdkey()){
	        String strID=entityKey.getObjectIdkey().toString();
	        IdToInsert.put("_id", entityKey.getObjectIdkey());
			  dbObject = new BasicDBObject("_id", new ObjectId(strID));
	        }
	        else if(entityKey.hasStringkey()){
	        	  IdToInsert.put("_id", entityKey.getStringkey());
				  dbObject = new BasicDBObject("_id", entityKey.getStringkey());
	        }
	        else if(entityKey.hasIntkey()){
		        IdToInsert.put("_id", entityKey.getIntkey());
				  dbObject = new BasicDBObject("_id", entityKey.getIntkey());
	        }
	        else  if(entityKey.haslistkey())
			  { String keys="";
			  for(int i=0;i<entityKey.getListkey().size();i++)
			  keys+=entityKey.getListkey().get(i).toString();
			  entityKey.setStringkey(keys);
			 // System.out.println("*******keys:"+keys);
			  IdToInsert.put("_id", keys);
			  dbObject = new BasicDBObject("_id", keys);
			  }	

		        // Add  entity properties:
			 int index=0;
			 while(index<propertyList.size()){
		   			//**************************************Check data type**************************************
			    	if(propertyList.get(index).getPropertyValue().haslongValue())
			    		{dbObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getLong());			    		
			    		}
			    	else if(propertyList.get(index).getPropertyValue().hasDoubleValue())
		    		{dbObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getDouble());
		    		}
			    	else if(propertyList.get(index).getPropertyValue().hasDateValue())
			    		{dbObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getDate());
			    		}
			    	else if(propertyList.get(index).getPropertyValue().hasArrayValue())	
			    		{ //BasicDBList dbList=setArrayPoroperties(propertyList.get(index).getPropertyName(),propertyList.get(index).getPropertyValue().getArray());
			    		int x=0;
			    		PropertyValue [] propertyValueArray=propertyList.get(index).getPropertyValue().getArray();
			    		ArrayList list = new ArrayList();
			    		while(x<propertyValueArray.length)
			    		{//*************Check data type*************
					    	if(propertyValueArray[x].haslongValue()) list.add(propertyValueArray[x].getLong());
					    	else if(propertyValueArray[x].hasDateValue()) list.add(propertyValueArray[x].getDate());
					    	else list.add(propertyValueArray[x].getString());
			    		x++;}
			    		dbObject.put(propertyList.get(index).getPropertyName(), list);
			    		}
			    	else
			    		{dbObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getString());
			    		}
			   
	        index++;

	        }
			 //--add the document--
		 coll.update(IdToInsert, dbObject,true,false);
		//	coll.insert(dbObject);
	     //}    
	}
	public void put(String entityType,List<Entities> entitiesList ) {
		
		 DBCollection coll = db.getCollection(entityType)	;   
		 List<DBObject> documents = new ArrayList<>();
		 List<DBObject> Catalogdocuments = new ArrayList<>();

		 for(int entityIndex=0;entityIndex<entitiesList.size();entityIndex++){
	       // ObjectId id = new ObjectId();
	        //BasicDBObject IdToInsert = new BasicDBObject();
	        DBObject dbObject =null;
			 EntityKey entityKey=entitiesList.get(entityIndex).getKey();
			 List<Properties> propertyList=entitiesList.get(entityIndex).getProperties();
		        BasicDBObject newObject =null;

	        String keyDataType="";
	    	//****************Check data type*************
	        if(entityKey.hasObjectIdkey()){
	        String strID=entityKey.getObjectIdkey().toString();
	       // newObject.put("_id", entityKey.getObjectIdkey());
	        newObject = new BasicDBObject("_id", new ObjectId(strID));
			  keyDataType="ObjectId";
	        }
	        else if(entityKey.hasStringkey()){
	        	//newObject.put("_id", entityKey.getStringkey());
	        	newObject = new BasicDBObject("_id", entityKey.getStringkey());
				  keyDataType="string";
	        }
	        else if(entityKey.hasIntkey()){
	        	//newObject.put("_id", entityKey.getIntkey());
	        	newObject = new BasicDBObject("_id", entityKey.getIntkey());
				  keyDataType="long";
	        }
	        else  if(entityKey.haslistkey())
			  { String keys="";
			  for(int i=0;i<entityKey.getListkey().size();i++)
			  keys+=entityKey.getListkey().get(i).toString();
			  
			  entityKey.setStringkey(keys);
			 // System.out.println("*******keys:"+keys);
			  //newObject.put("_id", keys);
			  newObject = new BasicDBObject("_id", keys);
			  keyDataType="string";
			  }	
	        //-----check if exsist_-----------
			   BasicDBObject catalogObject= new BasicDBObject ();
	    		catalogObject.put("KeyDataType",keyDataType);

		        // Add  entity properties:
			 int index=0;
			 while(index<propertyList.size()){
		   			//**************************************Check data type**************************************
			    	if(propertyList.get(index).getPropertyValue().haslongValue())
			    		{newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getLong());
			    		catalogObject.put(propertyList.get(index).getPropertyName(),"long");
			    		}
			    	else if(propertyList.get(index).getPropertyValue().hasDoubleValue())
		    		{newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getDouble());
		    		catalogObject.put(propertyList.get(index).getPropertyName(),"Double");
		    		}
			    	else if(propertyList.get(index).getPropertyValue().hasDateValue())
			    		{newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getDate());
			    		catalogObject.put(propertyList.get(index).getPropertyName(),"date");
			    		}
			    	else if(propertyList.get(index).getPropertyValue().hasArrayValue())	
			    		{ //BasicDBList dbList=setArrayPoroperties(propertyList.get(index).getPropertyName(),propertyList.get(index).getPropertyValue().getArray());
			    		catalogObject.put(propertyList.get(index).getPropertyName(),"array");
			    		int x=0;
			    		PropertyValue [] propertyValueArray=propertyList.get(index).getPropertyValue().getArray();
			    		ArrayList list = new ArrayList();
			    		while(x<propertyValueArray.length)
			    		{//*************Check data type*************
					    	if(propertyValueArray[x].haslongValue()) list.add(propertyValueArray[x].getLong());
					    	else if(propertyValueArray[x].hasDateValue()) list.add(propertyValueArray[x].getDate());
					    	else list.add(propertyValueArray[x].getString());
			    		x++;}
			    		newObject.put(propertyList.get(index).getPropertyName(), list);
			    		}
			    	else
			    		{newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getString());
			    		catalogObject.put(propertyList.get(index).getPropertyName(),"string");
			    		}
			   
	        index++;

	        }
		
			 //--add the document--
			 documents.add(newObject);
	     }//end for
	     coll.insert(documents);
	}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ) {
		BasicDBObject document = new BasicDBObject();

		DBCollection coll = db.getCollection(entityType)	;        
	        //______chech key type________
	        if(entityKey.hasObjectIdkey()){
	    		document.put("_id", entityKey.getObjectIdkey());
	        }
	        else if(entityKey.hasStringkey()){
	    		document.put("_id", entityKey.getStringkey());
	        }
		
		coll.remove(document);	
	//}else  System.out.println("Can't delete..Entity_Key not exist...");
		
	}
	/**
	 * <p>
	 * Delete EntityType. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		//*_Before delete, check if type exist 
		if(listofEntityTypes().contains(entityType)){
			DBCollection coll = db.getCollection(entityType)	; 
			coll.drop();
			System.out.println("Delete Entity Type called " + entityType + ".\n");
		}
	}
	/**
	 * <p>
	 * Create Entity Type. </p>
	 * 
	 */
	public void createEntityType(String entityType){
		 DBCollection coll = db.getCollection(entityType)	;    
		 System.out.println("Create.."+entityType);
		
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){

	Set<String> colls = db.getCollectionNames();

	List<String> list=new ArrayList<String>();
	
	for (String s : colls) {
		//if(s.contains("_Catalog")==false)
	list.add(s);
		//System.out.println("List:   "+s);
	//	}
	}
	return list;
	}
	//-------------------------------------------------------------------------------------
	private List<Entities> setEntityList(DBCursor cursor, String entityType){
		List<Entities> entList=new ArrayList<Entities> ();
		String field="";

	         for(int i=0;i<cursor.toArray().size();i++){
	             Entities entity=new Entities();

	        	Iterator<String> iter= cursor.toArray().get(i).keySet().iterator();
	        	 int x=0;
		         while(iter.hasNext()){
	        	 field=	 iter.next();
	        	 DBObject   doc= cursor.toArray().get(i);
	         if(field.compareTo("_id")==0)  
	         {    
	     	EntityKey key=new EntityKey(); 
	         String Keystr=""; 
	         Long IntKey=null;
	         		if(doc.get("_id").getClass().toString().contains("ObjectId"))
					         {ObjectId obj=(ObjectId) doc.get("_id");
					  		key.setObjectIdkey(obj);
					
					         }
	         		else if(doc.get("_id").getClass().toString().contains("int")||doc.get("_id").getClass().toString().contains("Long")||doc.get("_id").getClass().toString().contains("long"))	         				
		         		{IntKey=  Long.parseLong(doc.get("_id").toString());
		         		
	         			key.setIntkey(IntKey);  			}
	         		else
	         			{Keystr=  doc.get("_id").toString();
	         			key.setStringkey(Keystr);   
	         			}
	         entity.setKey(key);
	         entity.setEntityType(entityType);
	        }
	         else{ 
	         Properties property=new Properties();
	         PropertyValue propValue= new PropertyValue();
	   			//********************************Check data type*********************
		         if(doc.get(field).getClass().toString().contains("Long"))
		        	 propValue.setLong(Long.parseLong(doc.get(field).toString()));
		 
		        // else if(doc.get(field).toString().)
		         else  if(doc.get(field).getClass().toString().contains("BasicDBList"))
			         {BasicDBList l = (BasicDBList)doc.get(field);
			         propValue.setArray(setArrayValue(l));
			         }
			     else propValue.setString(doc.get(field).toString());
		         
		         property.setProperity(field, propValue);
	         entity.setProperties(property);

	         }
	        x++;}
		         entList.add(entity);
		         }
	    
	         return 	 entList;
}
	//____________________________________________________
	private PropertyValue [] setArrayValue(BasicDBList l){
		int i=0;
		PropertyValue []  lp=	new PropertyValue [l.size()];

		while(i<l.size()){
			 lp[i]=new PropertyValue();

			 if(l.get(i).getClass().toString().contains("Long"))
				 lp[i].setLong(Long.parseLong(l.get(i).toString()));
	
			 else
				 lp[i].setString(l.get(i).toString());
		i++;}
		return lp;
	}
	
	
}










package main.java.api;


import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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
	    DBCollection collection = db.getCollection(entityType);
		DBCursor cursor=null;
		// find the entities that match the condition and specified fields.
		cursor = collection.find(condition,fields);//(It  works Even if there is condition and fields=null)
		 
		//--------check if no result-----------------------
		 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);}
		 // call the 'setEntityList' method to  set all retrieved entities in a list
		 List<Entities>   entList= setEntityList( cursor,  entityType);
		 queryResponse.setEntities(entList);// set the list to the response object, and then return it.
		
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
		 DBCollection coll = db.getCollection(entityType);
        BasicDBObject IdToInsert = new BasicDBObject();
        if(entityKey.hasObjectIdkey())IdToInsert.put("_id", entityKey.getObjectIdkey());
        else if( entityKey.hasStringkey())IdToInsert.put("_id", entityKey.getStringkey());
        	
        DBCursor cursor = coll.find(IdToInsert);
      //--------check if no result-----------------------
		 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);;}
		
     List<Entities>   entList= setEntityList( cursor,  entityType);
     List<Properties>  propList= entList.get(0).getProperties();
     resp.setProperties(propList);;
       
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
	List<Entities> entList=new ArrayList<Entities> ();
	
		  DBCollection coll = db.getCollection(entityType);
	         DBCursor cursor = coll.find();
	       //--------check if no result-----------------------
			 if (cursor.count()==0){System.out.print("No result match the query"); System.exit(1);;}

	         entList= setEntityList( cursor,  entityType);
	         
		Response rsp=new Response();
		rsp.setEntities(entList);

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
	        //-----check if exsist_-----------
	        //--if the entity key is found and user does not want to replace it, so nothing will be changed..
	     if(coll.find(IdToInsert).hasNext()&&!ReplaceIfExist);
	     else{
	    	   if(coll.find(IdToInsert).hasNext()&&ReplaceIfExist) //-- if it is founded and user wants to replace it, so delete this entity.
	    		   deleteEntity(entityType,entityKey);
			   if(!coll.find(IdToInsert).hasNext())
				   coll.insert(IdToInsert);	
			   //---find the id that inserted now, or that exists before to put fields to it.
			   BasicDBObject newObject = (BasicDBObject) coll.findOne(dbObject);
			   //----------------------------------------
		        // Add  entity properties:
			 int index=0;
			 while(index<propertyList.size()){
		   			//**************************************Check data type**************************************
			    	if(propertyList.get(index).getPropertyValue().haslongValue())
			    		newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getLong()); 
			    	else if(propertyList.get(index).getPropertyValue().hasDateValue())
			    		newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getDate());
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
			    		newObject.put(propertyList.get(index).getPropertyName(), list);
			    		}
			    	else
			    		newObject.put(propertyList.get(index).getPropertyName(), propertyList.get(index).getPropertyValue().getString());
			   
	        index++;

	        }
			 //--add the document--
		 coll.findAndModify(dbObject, newObject);
	     }    
	}
	
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ) {
		DBCollection coll = db.getCollection(entityType)	;        
		BasicDBObject document = new BasicDBObject();
	        //______chech key type________
	        if(entityKey.hasObjectIdkey()){
	    		document.put("_id", entityKey.getObjectIdkey());
	        }
	        else if(entityKey.hasStringkey()){
	    		document.put("_id", entityKey.getStringkey());
	        }
		
		coll.remove(document);	
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
	         int IntKey=0;
	         		if(doc.get("_id").getClass().toString().contains("ObjectId"))
					         {ObjectId obj=(ObjectId) doc.get("_id");
					  		key.setObjectIdkey(obj);
					
					         }
	         		else if(doc.get("_id").getClass().toString().contains("int")||doc.get("_id").getClass().toString().contains("long"))	         				
		         		{IntKey=  Integer.parseInt(doc.get("_id").toString());
	         			key.setIntkey(IntKey);;	         			}
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
		        /* else  if(doc.get(field).getClass().toString().contains("Date"))
		         {DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
					 Date date=null;
					try {
						date = dateFormat.parse( doc.get(field).toString());
					} catch (ParseException e) {
						e.printStackTrace();
					}
				 propValue.setDate(date);
		         }*/
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
			/* else if (l.get(i).getClass().toString().contains("Date"))
			 {DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
			 Date date=null;
			try {
				date = dateFormat.parse( l.get(i).toString());
			} catch (ParseException e) {
				e.printStackTrace();
			} */
			 else
				 lp[i].setString(l.get(i).toString());
		i++;}
		return lp;
	}
	
	
}










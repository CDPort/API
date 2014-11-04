package main.java.api;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;


import com.google.api.services.datastore.DatastoreV1.BeginTransactionRequest;
import com.google.api.services.datastore.DatastoreV1.BeginTransactionResponse;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.GqlQuery;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.DatastoreV1.Mutation;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.protobuf.ByteString;


/**
 * 
 *<p>
 * GoogleDatastore class has implementation for the main Functions: </p>
 * <p><i>
 * Connect, Query, getProperty,getEntity, put, deleteEntity</p></i>
 * 
 * @CDport 1.0
 * 
 */

public class GoogleDatastore 
{//Google Datastore object
	private static Datastore datastore=null ;

	/**
	 *Constructor.
	 * 
	 * @param
	 */
	public GoogleDatastore(){
		super();
	}

	/**
	 * Connect to Google Cloud Datastore.
	 * 
	 *  @param
	 *  
	 */
	
	public void connectToDatastore(String datasetId) {
		try {
	      // Setup the connection to Google Cloud Datastore and infer credentials
	      // from the environment.
	    	
	      datastore = DatastoreFactory.get().create(DatastoreHelper.getOptionsfromEnv()
	          .dataset(datasetId).build());
	    
	    } catch (GeneralSecurityException exception) {
	      System.err.println("Security error connecting to the datastore: " + exception.getMessage());
	      System.exit(1);
	    } catch (IOException exception) {
	      System.err.println("I/O error connecting to the datastore: " + exception.getMessage());
	      System.exit(1);
	    }
	    
	      System.out.println("===========================================");
	      System.out.println("Getting Started with Google Datastore");
	      System.out.println("===========================================\n");	
	      
	}
	
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple Queries</p>
	 * The retrieved statement will be converted into GQL Query.
	 *
	 * @param QueryStatement the string query statement. 
	 * 
	 * @return  Response 
	 */
		
public Response query(String QueryStatement) {//*****does not check if  entity type exist.....
	  	  Response rsp=new Response();
	try{
     //--The retrieved Datastore entities, will be converted into the CDPOrt entities and then set to the Response object to be returned
  	List<Entities> entities=new ArrayList<Entities>();
		
	 GqlQuery.Builder query = GqlQuery.newBuilder().setQueryString(QueryStatement);
	 query.setAllowLiteral(true);

	 
     RunQueryRequest request = RunQueryRequest.newBuilder().setGqlQuery(query).build();
	 RunQueryResponse response = datastore.runQuery(request);
	  
		  
	   //--check if there is no result
		 if (response.getBatch().getEntityResultList().size()==0){System.out.print("No result match the query"); System.exit(1);;}
		 
		 //--list of entities retrieved from the Google Datatsore after executing the query
	      List<Entity> getresults = new ArrayList<Entity>();
		     int count=0;// entity counter
	      for (EntityResult entityResult : response.getBatch().getEntityResultList()) {
		        getresults.add(entityResult.getEntity());
		      
		        EntityKey entKey=new EntityKey();
		        String type=getresults.get(count).getKey().getPathElementList().get(0).getKind();
		        String entityname=getresults.get(count).getKey().getPathElementList().get(0).getName();
		        long entityid=getresults.get(count).getKey().getPathElementList().get(0).getId();//<<id
		      //**************************************Check data type
		       if(entityname!= "") 
		    	   entKey.setStringkey(entityname);	
		       else  if(entityid!= 0)
		    	   entKey.setIntkey(entityid); 
		       else {System.out.println("error in the retrived key");   System.exit(1);}
		       
			       Entities entity=new Entities();  
			       entity.setEntityType(type);//--set type
			       entity.setKey(entKey);//--set key

			       for(int i=0; i<getresults.get(count).getPropertyCount();i++)//get all properties in the entity
		        {
		        	String ProbName=  getresults.get(count).getProperty(i).getName();
		        	PropertyValue Probvalue= new PropertyValue();
		   		//**************************************Check data type**************************************
		        	if(getresults.get(count).getProperty(i).getValue().hasIntegerValue())
	        		Probvalue.setLong(getresults.get(count).getProperty(i).getValue().getIntegerValue() ); 	
		        	else if(	getresults.get(count).getProperty(i).getValue().hasDoubleValue())
		        	  Probvalue.setDouble( getresults.get(count).getProperty(i).getValue().getDoubleValue() );   
		        	else if(getresults.get(count).getProperty(i).getValue().hasBooleanValue())
		        		Probvalue.setBoolean(getresults.get(count).getProperty(i).getValue().getBooleanValue());
		        	else //string
		        		Probvalue.setString( getresults.get(count).getProperty(i).getValue().getStringValue());
		        //-------check array-----
		        	if(getresults.get(count).getProperty(i).getValue().getListValueList().size()>0)
			         Probvalue.setArray(setArrayValue(getresults.get(count).getProperty(i).getValue().getListValueList()));
	        		
		        	//--set properties
		        	Properties property= new Properties();
					property.setProperity(ProbName, Probvalue);
				 entity.setProperties(property);
		        }
	
		 entities.add(entity);
		 count++;
	}
	
		rsp.setEntities(entities);//--set entities list to the response

		 } catch (DatastoreException exception) {
		      // Catch all Datastore rpc errors.
		      System.err.println("Error while doing datastore operation");
		      exception.printStackTrace();
		      System.exit(1);
		    }catch(Exception e){ ;}

  	  return rsp;	
	}
	
	/**
	 * <p> Get Property.</p>
	 * <p>
	 * Get all properties that belong to the <i> entity type </i>and the <i>Entity Key</i> that are specified.</p>
	 * EntityKey can be <b>long</b> or <b> string </b>.
	 * 
	 * @return Response
	 */
	
	
	public Response getProperty (String entityType,EntityKey entityKey) {
		Key.Builder key=null;
		//**************************************Check data type	
		  if(entityKey.getIntkey()>0){
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType).setId(entityKey.getIntkey()));
		}
		  else  if(entityKey.haslistkey())
		  { String keys="";
		  for(int i=0;i<entityKey.getListkey().size();i++)
		  keys+=entityKey.getListkey().get(i).toString();
		 // System.out.println("*******keys:"+keys);
		  key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(keys));
		  }	  else{
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType)
			          .setName(entityKey.getStringkey()));
			}
		   Entity    entity =null;
	     
	    try{
	    	//--lookup for the entity has the specified key
	    LookupRequest request = LookupRequest.newBuilder().addKey(key).build();
	    LookupResponse response = datastore.lookup(request);
	    if (response.getMissingCount() == 1) {
	      throw new RuntimeException("Entity not found");
	    }
	     entity = response.getFound(0).getEntity();
	    }catch(Exception e){ ;}
	    
	       List<Property> DSprop =   entity.getPropertyList();
 //-----get all properties-----
	int index=0;
	Response rp=new Response();
	List<Properties> propList=new ArrayList<Properties>();
	while(index<DSprop.size()){
		String name=DSprop.get(index).getName();
    	PropertyValue value= new PropertyValue();
		//***********************Check data type********
    	if(DSprop.get(index).getValue().hasIntegerValue())
    		value.setLong(DSprop.get(index).getValue().getIntegerValue());
    	else if(DSprop.get(index).getValue().hasDoubleValue())
    		value.setDouble(DSprop.get(index).getValue().getDoubleValue()); 
    	else if(DSprop.get(index).getValue().hasBooleanValue())
    		value.setBoolean(DSprop.get(index).getValue().getBooleanValue());
    	else //string
    		{value.setString(DSprop.get(index).getValue().getStringValue());
    	    }			
    //-------check array-----
    if(DSprop.get(index).getValue().getListValueList().size()>0)
    {value.setArray(setArrayValue(DSprop.get(index).getValue().getListValueList()));
    }		
		Properties prop=new Properties();
		prop.setProperity(name, value);
	propList.add(prop);
	index++;}
	rp.setProperties(propList);
	return rp;
}
	
	/**
	 * <p> Get Entity.</p>
	 * <p>
	 * Get all Entities that belong to the specified <i> Entity type </i>.</p>
	 * EntityKey can be <b>long</b> or <b> String </b>.
	 * 
	 * @return Response
	 * 
	 */
	
	public Response getEntity(String entityType) {
		String QueryStatement="select * from `" +entityType+ "`" ;
		Response rsp= query(QueryStatement);
		return rsp;
	}
	//....
		public Response getEntity(String entityType, int from, int to) {
		Response rsp=new Response();
		String q="";
		if(from==0)
			 q="select * from `"+entityType+"` limit "+ to;
		else
		 q="select * from `"+entityType+"` limit "+ to +" offset "+ from ;
		System.out.println("Query="+q);
		rsp=query(q);
		return rsp;
		}
//.....
	public Response getEntity(String entityType ,List<EntityKey> keys) {
		Key.Builder key=null;
	 	//--lookup for the entity has the specified key
	    LookupRequest.Builder req = LookupRequest.newBuilder();
	  
		for(int keyIndex=0;keyIndex<keys.size();keyIndex++){
			EntityKey k=keys.get(keyIndex);
		
		//Check data type
		  if(k.getIntkey()>0){
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType).setId(k.getIntkey()));
		}
		  else  if(k.haslistkey())
		  { String keystr="";
		  for(int i=0;i<k.getListkey().size();i++)
		  keystr+=k.getListkey().get(i).toString();
		  key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(keystr));
		  }	  else{
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType)
			          .setName(k.getStringkey()));
			}
		
		  req.addKey(key);
		}//end for
		   LookupRequest request=req.build();
	

		 List<EntityResult>   entityList =new ArrayList<EntityResult>();
	     
	    try{
	    LookupResponse response = datastore.lookup(request);
	    if (response.getMissingCount() == 1) {
	      throw new RuntimeException("Entity not found");
	    }
	    entityList = response.getFoundList();
	    }catch(Exception e){ ;}
	    
	    
		Response rp=new Response();
		List<Entities> entities=new ArrayList<Entities>();
	    for(int entityIndex=0;entityIndex<entityList.size();entityIndex++){
	       List<Property> DSprop =   entityList.get(entityIndex).getEntity().getPropertyList();
		    Entities e=new Entities();

 //-----get all properties-----
	int index=0;

	while(index<DSprop.size()){
		String name=DSprop.get(index).getName();
    	PropertyValue value= new PropertyValue();
		//***********************Check data type********
    	if(DSprop.get(index).getValue().hasIntegerValue())
    		value.setLong(DSprop.get(index).getValue().getIntegerValue());
    	else if(DSprop.get(index).getValue().hasDoubleValue())
    		value.setDouble(DSprop.get(index).getValue().getDoubleValue()); 
    	else if(DSprop.get(index).getValue().hasBooleanValue())
    		value.setBoolean(DSprop.get(index).getValue().getBooleanValue());
    	else //string
    		{value.setString(DSprop.get(index).getValue().getStringValue());
    	    }		
    		
    //-------check array-----
    if(DSprop.get(index).getValue().getListValueList().size()>0)
    {value.setArray(setArrayValue(DSprop.get(index).getValue().getListValueList()));
    }		
		Properties prop=new Properties();
		prop.setProperity(name, value);
	//propList.add(prop);
	index++;
	e.setProperties(prop);////set prop
	
	}
	e.setEntityType(entityType);/// set entity type
	EntityKey k= new EntityKey();
	long intkey=entityList.get(entityIndex).getEntity().getKey().getPathElement(0).getId();
	String strkey=entityList.get(entityIndex).getEntity().getKey().getPathElement(0).getName();
    if(strkey!= "") 
 	   k.setStringkey(strkey);	
    else  if(intkey!= 0)
 	   k.setIntkey(intkey); 
	e.setKey(k); /// set key
	entities.add(e);
 }//end for
	rp.setEntities(entities);;
	return rp;
	}
	/**
	 * <p> Put.</p>
	 * <p>
	 * To add new Entity. The method also supports replace entity if exist. </p>
	 * 
	 * @param entityType String. (It is the 'Kind' name in the Google Datastore).
	 * @param entityKey (It can be <b>long</b> or <b> String </b>)
	 * @param propertyList The list of properties of the new entity. The property value can be a <b>String, long, double, boolean or an array of any of these types</b>.
	 * @param ReplaceIfExist (boolean variable). Set it to <b> true </b> if you want to replace the entity if it exists. 
	 *   	Then, the entire entity properties will be replaces by the new entity properties. 
	 *   	If it <b> false </b> and the entity key is exist, so no changes will be performed even if the properties list is different.
	 *
	 */
	public void put(String entityType,EntityKey entityKey,List<Properties> propertyList,boolean ReplaceIfExist )   {
		try{
		
		// Create an RPC request to begin a new transaction.
    BeginTransactionRequest.Builder   treq = BeginTransactionRequest.newBuilder();
	      // Execute the RPC synchronously.
    BeginTransactionResponse    tres = datastore.beginTransaction(treq.build());
	      // Get the transaction handle from the response.
	 	 ByteString      tx = tres.getTransaction();

	      // Create an RPC request to get entities by key.
	 	LookupRequest.Builder    lreq = LookupRequest.newBuilder();
	      // Set the entity key with only one `path_element`: no parent.
	 	Key.Builder key= null;
	 	//**************************************Check data type
		if(entityKey.hasStringkey()){		

			key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(entityKey.getStringkey()));
		}
		else  if(entityKey.hasIntkey()){
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType).setId(entityKey.getIntkey()));

		}
		  else  if(entityKey.haslistkey())
		  { String keys="";
		  for(int i=0;i<entityKey.getListkey().size();i++)
		  keys+=entityKey.getListkey().get(i).toString();
		  
		  entityKey.setStringkey(keys);
		 // System.out.println("*******keys:"+keys);
		  key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(keys));
		  }	
		else System.out.println("Error:you don't set Entity Key"); 

	      // Add one key to the lookup request.
	      lreq.addKey(key);
	      // Set the transaction, so we get a consistent snapshot of the
	      // entity at the time the transaction started.
	      lreq.getReadOptionsBuilder().setTransaction(tx);
	      // Execute the RPC and get the response.
	      LookupResponse   lresp = datastore.lookup(lreq.build());
	     
	      Entity entity; Entity.Builder entityBuilder=null;
	      if (lresp.getFoundCount() > 0) {
	        entity = lresp.getFound(0).getEntity(); 
	         entityBuilder = Entity.newBuilder(entity);
	        entityBuilder.clearProperty();
	      }
	        else {
	        // If no entity was found, create a new one.
	       entityBuilder = Entity.newBuilder();
	        // Set the entity key.
	        entityBuilder.setKey(key);
	        }
	        //----------------------------------------
	        // Add  entity properties:

			 int index=0;
			 while(index<propertyList.size()){
		   	//****************Check data type*****************
			 if(propertyList.get(index).getPropertyValue().haslongValue())
			 { 
				 entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setIntegerValue(propertyList.get(index).getPropertyValue().getLong())));
			 } else if(propertyList.get(index).getPropertyValue().hasDoubleValue())
			 {  entityBuilder.addProperty(Property.newBuilder()
				            .setName(propertyList.get(index).getPropertyName())
				            .setValue(Value.newBuilder().setDoubleValue(propertyList.get(index).getPropertyValue().getDouble())));		
			 }else if(propertyList.get(index).getPropertyValue().hasbooleanValue())
			 { 	entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setBooleanValue(propertyList.get(index).getPropertyValue().getBoolean())));
			  	
			 }
			 else if(propertyList.get(index).getPropertyValue().hasDateValue())
			 { 	entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setStringValue(propertyList.get(index).getPropertyValue().getDate().toString())));
			  	
			 }
			 else if(propertyList.get(index).getPropertyValue().hasArrayValue())
			 {
				  	int x=0;
			    		PropertyValue [] propertyValueArray=propertyList.get(index).getPropertyValue().getArray();
						 Value.Builder listValueBuilder = Value.newBuilder();
			    		while(x<propertyValueArray.length)
			    		{//*************Check data type*************
					    	if(propertyValueArray[x].haslongValue())   listValueBuilder.addListValue(Value.newBuilder().setIntegerValue(propertyValueArray[x].getLong()));
					    	else if(propertyValueArray[x].hasDoubleValue()) listValueBuilder.addListValue(Value.newBuilder().setDoubleValue(propertyValueArray[x].getDouble()));
					    	else if(propertyValueArray[x].hasbooleanValue()) listValueBuilder.addListValue(Value.newBuilder().setBooleanValue(propertyValueArray[x].getBoolean()));
					    	else listValueBuilder.addListValue(Value.newBuilder().setStringValue(propertyValueArray[x].getString()));
			    		x++;}
	
						 entityBuilder.addProperty(Property.newBuilder()
						     .setName(propertyList.get(index).getPropertyName())
						     .setValue(listValueBuilder));
						   	
				 }
			   	else //string { 
			   	{
				   entityBuilder.addProperty(Property.newBuilder()
						      .setName(propertyList.get(index).getPropertyName())
						       .setValue(Value.newBuilder().setStringValue(propertyList.get(index).getPropertyValue().getString())));	
			   	}			 
	        index++;
			 }
			
					
	        // Build the entity.
	        entity = entityBuilder.build();
	      
	        CommitRequest request =null;
	        if(ReplaceIfExist){ request = CommitRequest.newBuilder()
				    .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
				    .setMutation(Mutation.newBuilder().addUpsert(entity))
				    .build();
	        	}
	        else{
	        	 request = CommitRequest.newBuilder()
		        	    .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
		        	    .setMutation(Mutation.newBuilder().addInsert(entity))
		        	    .build();//using addInsert<< to enable put without replace............
	        }
	        	datastore.commit(request); 
	        
	} catch (DatastoreException exception) {;}
	}
	////////////////////////////////////////////////////////////////
	public void put(String entityType,List<Entities> entityList )   {
		try{
		     CommitRequest.Builder request = request = CommitRequest.newBuilder();
     	    request.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
     	    Mutation.Builder m=    Mutation.newBuilder();
     
		// Create an RPC request to begin a new transaction.
    BeginTransactionRequest.Builder   treq = BeginTransactionRequest.newBuilder();
	      // Execute the RPC synchronously.
    BeginTransactionResponse    tres = datastore.beginTransaction(treq.build());
	      // Get the transaction handle from the response.
	 	 ByteString      tx = tres.getTransaction();

	      // Create an RPC request to get entities by key.
	 	LookupRequest.Builder    lreq = LookupRequest.newBuilder();
	      // Set the entity key with only one `path_element`: no parent.
	 	
	 	for(int entityIndex=0;entityIndex<entityList.size();entityIndex++){
	 	EntityKey entityKey=entityList.get(entityIndex).getKey();
	 	List<Properties> propertyList=entityList.get(entityIndex).getProperties();
	 	
	 	Key.Builder key= null;
	 	//**************************************Check data type
		if(entityKey.hasStringkey()){		

			key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(entityKey.getStringkey()));
		}
		else  if(entityKey.hasIntkey()){
			key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType).setId(entityKey.getIntkey()));

		}
		  else  if(entityKey.haslistkey())
		  { String keys="";
		  for(int i=0;i<entityKey.getListkey().size();i++)
		  keys+=entityKey.getListkey().get(i).toString();
		  
		  entityKey.setStringkey(keys);
		 // System.out.println("*******keys:"+keys);
		  key = Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)
		          .setName(keys));
		  }	
		else System.out.println("Error:you don't set Entity Key"); 

	      // Add one key to the lookup request.
	      lreq.addKey(key);
	      // Set the transaction, so we get a consistent snapshot of the
	      // entity at the time the transaction started.
	      lreq.getReadOptionsBuilder().setTransaction(tx);
	      // Execute the RPC and get the response.
	      LookupResponse   lresp = datastore.lookup(lreq.build());
	     
	      Entity entity; Entity.Builder entityBuilder=null;
	      if (lresp.getFoundCount() > 0) {
	        entity = lresp.getFound(0).getEntity(); 
	         entityBuilder = Entity.newBuilder(entity);
	        entityBuilder.clearProperty();
	      }
	        else {
	        // If no entity was found, create a new one.
	       entityBuilder = Entity.newBuilder();
	        // Set the entity key.
	        entityBuilder.setKey(key);
	        }
	        //----------------------------------------
	        // Add  entity properties:

			 int index=0;
			 while(index<propertyList.size()){
		   	//**************************************Check data type**************************************
			 if(propertyList.get(index).getPropertyValue().haslongValue())
			 { 
				 entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setIntegerValue(propertyList.get(index).getPropertyValue().getLong())));
			 } else if(propertyList.get(index).getPropertyValue().hasDoubleValue())
			 {  entityBuilder.addProperty(Property.newBuilder()
				            .setName(propertyList.get(index).getPropertyName())
				            .setValue(Value.newBuilder().setDoubleValue(propertyList.get(index).getPropertyValue().getDouble())));		
			 }else if(propertyList.get(index).getPropertyValue().hasbooleanValue())
			 { entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setBooleanValue(propertyList.get(index).getPropertyValue().getBoolean())));
			  	
			 }
			 else if(propertyList.get(index).getPropertyValue().hasDateValue())
			 { entityBuilder.addProperty(Property.newBuilder()
					          .setName(propertyList.get(index).getPropertyName())
					          .setValue(Value.newBuilder().setStringValue(propertyList.get(index).getPropertyValue().getDate().toString())));
			  	
			 }
			 else if(propertyList.get(index).getPropertyValue().hasArrayValue())
			 {	int x=0;
			    		PropertyValue [] propertyValueArray=propertyList.get(index).getPropertyValue().getArray();
						 Value.Builder listValueBuilder = Value.newBuilder();
			    		while(x<propertyValueArray.length)
			    		{//*************Check data type*************
					    	if(propertyValueArray[x].haslongValue())   listValueBuilder.addListValue(Value.newBuilder().setIntegerValue(propertyValueArray[x].getLong()));
					    	else if(propertyValueArray[x].hasDoubleValue()) listValueBuilder.addListValue(Value.newBuilder().setDoubleValue(propertyValueArray[x].getDouble()));
					    	else if(propertyValueArray[x].hasbooleanValue()) listValueBuilder.addListValue(Value.newBuilder().setBooleanValue(propertyValueArray[x].getBoolean()));
					    	else listValueBuilder.addListValue(Value.newBuilder().setStringValue(propertyValueArray[x].getString()));
			    		x++;}
	
						 entityBuilder.addProperty(Property.newBuilder()
						     .setName(propertyList.get(index).getPropertyName())
						     .setValue(listValueBuilder));
						  // 	}
				 }
			   	else 
			   	{
				   entityBuilder.addProperty(Property.newBuilder()
						      .setName(propertyList.get(index).getPropertyName())
						       .setValue(Value.newBuilder().setStringValue(propertyList.get(index).getPropertyValue().getString())));	
			   	}			 
	        index++;
			 }	
	        // Build the entity.
	        entity = entityBuilder.build();
		        m.addInsert(entity);
	 	}//end for
		        request.setMutation(m);
		       CommitRequest req=   request .build();//using addInsert<< to enable put without replace............
	        	datastore.commit(req); 
	        
	} catch (DatastoreException exception) {;}
	}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ) {
		try{
	
		 	Key.Builder key= null;
			//***********************************Check data type
			if(entityKey.hasStringkey()){		
				key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType)
			          .setName(entityKey.getStringkey()));
			}
			else  if(entityKey.hasIntkey()){
				key = Key.newBuilder().addPathElement(
				          Key.PathElement.newBuilder()
				          .setKind(entityType).setId(entityKey.getIntkey()));

			}
			else System.out.println("Error:you don't set Entity Key"); 

		     
	CommitRequest request = CommitRequest.newBuilder()
	    .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
	    .setMutation(Mutation.newBuilder().addDelete(key))
	    .build();
	datastore.commit(request);
		} catch (DatastoreException exception) {;}
		}
	/**
	 * <p>
	 * Delete EntityType. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
	Response resp= query("select __key__ from "+ entityType);
	for (int x=0;x<resp.getEntities().size();x++){
		EntityKey entityKey=resp.getEntities().get(x).getKey();
			
		try{
		 	Key.Builder key= null;
			//Check data type
			if(entityKey.hasStringkey()){		
				key = Key.newBuilder().addPathElement(
			          Key.PathElement.newBuilder()
			          .setKind(entityType)
			          .setName(entityKey.getStringkey()));
			}
			else  if(entityKey.hasIntkey()){
				key = Key.newBuilder().addPathElement(
				          Key.PathElement.newBuilder()
				          .setKind(entityType).setId(entityKey.getIntkey()));

			}
			else System.out.println("Error:you don't set Entity Key"); 

		     
	CommitRequest request = CommitRequest.newBuilder()
	    .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
	    .setMutation(Mutation.newBuilder().addDelete(key))
	    .build();
	datastore.commit(request);
		} catch (DatastoreException exception) {;}
		System.out.println(x+"Delete Entity Type called " + entityType + ".\n");
	}//end for
	}
	/**
	 * <p>
	 * Create Entity Type. </p>
	 * 
	 */
	public void createEntityType(String entityType){
		try{
			
			// Create an RPC request to begin a new transaction.
	    BeginTransactionRequest.Builder   treq = BeginTransactionRequest.newBuilder();
		      // Execute the RPC synchronously.
	    BeginTransactionResponse    tres = datastore.beginTransaction(treq.build());
		      // Get the transaction handle from the response.
		 ByteString      tx = tres.getTransaction();
		      // Create an RPC request to get entities by key.
		 LookupRequest.Builder    lreq = LookupRequest.newBuilder();
		 
		 Key.newBuilder().addPathElement(
		          Key.PathElement.newBuilder()
		          .setKind(entityType)); 
		   lreq.getReadOptionsBuilder().setTransaction(tx);
		   System.out.println("Create.."+entityType);
		   
		} catch (DatastoreException e) {
			e.printStackTrace();
		}
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		List<String> list=new ArrayList<String>();
		
	try{
		GqlQuery.Builder query = GqlQuery.newBuilder().setQueryString("SELECT * FROM __kind__");
		 query.setAllowLiteral(true);
	     RunQueryRequest request = RunQueryRequest.newBuilder().setGqlQuery(query).build();
		 RunQueryResponse response = datastore.runQuery(request);
		   //--check if there is no result
			 if (response.getBatch().getEntityResultList().size()==0){System.out.print("No result match the query"); System.exit(1);;}
			 
			 //--list of entities retrieved from the Google Datatsore after executing the query
		      List<Entity> getresults = new ArrayList<Entity>();
			     int count=0;// entity counter
		      for (EntityResult entityResult : response.getBatch().getEntityResultList()) {
			        getresults.add(entityResult.getEntity());
			        String type=entityResult.getEntity().getKey().getPathElementList().get(0).getName();
			        if(type.startsWith("__")==false&&type.startsWith("_")==false)
			        list.add(count,type); 	
			        }
		} catch (DatastoreException exception) {
	      // Catch all Datastore rpc errors.
	      System.err.println("Error while doing datastore operation");
	      exception.printStackTrace();
	      System.exit(1);
	    	}catch(Exception e){ ;}
	
		return list;
		}

	
	//____________________________________________________
		private PropertyValue [] setArrayValue(List<Value> l){
			int i=0;
			PropertyValue []  lp=	new PropertyValue [l.size()];
				
			while(i<l.size()){
				 lp[i]=new PropertyValue();

				 if(l.get(i).hasIntegerValue())
					 lp[i].setLong(l.get(i).getIntegerValue());
				 else  if(l.get(i).hasDoubleValue())
					 lp[i].setDouble(l.get(i).getDoubleValue());
				 else  if(l.get(i).hasBooleanValue())
					 lp[i].setBoolean(l.get(i).getBooleanValue());
				 else
					 lp[i].setString(l.get(i).getStringValue());
			i++;}
			return lp;
		}
		
		
}


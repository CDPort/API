package main.java.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.SelectResult;



/**
 * 
 *<p>
 * SimpleDB class has implementation for the main Functions: </p>
 * <p><i>
 * Connect, Query, getProperty,getEntity, put, deleteEntity</p></i>
 * 
 * @CDport 1.0
 * 
 */

public class SimpleDB 
{	
	private static  AmazonSimpleDB sdb;

	/**
	 *Constructor.
	 * 
	 * @param
	 */
	public SimpleDB(){
		super();
	}

	/**
	 * Connect to Amazon SimpleDB.
	 * 
	 *  @param
	 *  
	 */
	
	public void connectToSimpleDB(String accessKey, String secretKey, String regionName ) {
		//--set the credentials--
		 AWSCredentials credentials  = new BasicAWSCredentials(accessKey, secretKey);
	     sdb = new AmazonSimpleDBClient(credentials);
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
		sdb.setRegion(r);

	    System.out.println("===========================================");
	    System.out.println("Getting Started with Amazon SimpleDB");
	    System.out.println("===========================================\n");
	}
	
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple select Queries, like (select * from type)</p>
	 *
	 * @param QueryStatement the string query statement. 
	 * @param
	 * 
	 * @return  Response 
	 */
	public Response query(String QueryStatement, String entityType) {
		Response rsp=new Response();
	  	List<Entities> entities=new ArrayList<Entities>();
		//*_Before execute query, check  entityType exist.!!
	 String nextToken = null;
	 do{
		SelectRequest selectRequest = new SelectRequest(QueryStatement);
		if(nextToken != null){
            selectRequest.setNextToken(nextToken);
        }
		SelectResult result = sdb.select(selectRequest);
		//-------- check if no result -----------------------
		if(result.getItems().size()==0){System.out.print("No result match the query"); System.exit(1);;}
		for (Item item : result.getItems()) {
		String entityName=	item.getName();
	    EntityKey entKey=new EntityKey();
		entKey.setStringkey(entityName);
		//--set key and type..
		Entities entity=new Entities();
		entity.setEntityType(entityType);
		entity.setKey(entKey);
		//--set properties
		 for (Attribute attribute : item.getAttributes()) {
			 String attrName=attribute.getName();
			 PropertyValue attrValue=new PropertyValue();
			 attrValue.setString(attribute.getValue());
				Properties property= new Properties();
				property.setProperity(attrName, attrValue);
			 entity.setProperties(property);
	
			 }
			 //add entity to the list
			 entities.add(entity);
			}
		nextToken = result.getNextToken();
	 }while(nextToken != null);
	rsp.setEntities(entities);

  	  return rsp;	
	}
	public Response query2(String QueryStatement, String entityType) {
		Response rsp=new Response();
	  	List<Entities> entities=new ArrayList<Entities>();
		//*_Before execute query, check  entityType exist.!!
	if(listofEntityTypes().contains(entityType))
	{
	 String nextToken = null;
	 //do{
		SelectRequest selectRequest = new SelectRequest(QueryStatement);
		if(nextToken != null){
            selectRequest.setNextToken(nextToken);
        }
		SelectResult result = sdb.select(selectRequest);
		//-------- check if no result -----------------------
		if(result.getItems().size()==0){System.out.print("No result match the query"); System.exit(1);;}
		for (Item item : result.getItems()) {
		String entityName=	item.getName();
	    EntityKey entKey=new EntityKey();
		entKey.setStringkey(entityName);
		//--set key and type..
		Entities entity=new Entities();
		entity.setEntityType(entityType);
		entity.setKey(entKey);
		//--set properties
		 for (Attribute attribute : item.getAttributes()) {
			 String attrName=attribute.getName();
			 PropertyValue attrValue=new PropertyValue();
			 attrValue.setString(attribute.getValue());
				Properties property= new Properties();
				property.setProperity(attrName, attrValue);
			 entity.setProperties(property);
	
			 }
			 //add entity to the list
			 entities.add(entity);
			}

	}		else System.out.println("Can't execute query..Entity Type not exist...");
	rsp.setEntities(entities);

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
		//*_Before execute query, check  entityType exist.!!
	if(listofEntityTypes().contains(entityType))
	{
			GetAttributesRequest greq =new GetAttributesRequest();
		greq.setDomainName(entityType);
		greq.setItemName(entityKey.getStringkey());
		GetAttributesResult gres=	sdb.getAttributes(greq);
		List<Attribute> atrr=	gres.getAttributes();
			
		int index=0;
		List<Properties> propList=new ArrayList<Properties>();
		while(index<atrr.size()){
			String name=atrr.get(index).getName();
			 PropertyValue value=new PropertyValue();
		 value.setString(atrr.get(index).getValue());
			Properties prop=new Properties();
			prop.setProperity(name, value);
		propList.add(prop);
		index++;}
		rp.setProperties(propList);
	}		else System.out.println("Can't get_Properties..Entity Type not exist...");

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
		String QueryStatement="select * from `" +entityType+ "`" ;
		Response rsp=new Response();
		//*_Before execute query, check  entityType exist.!!
		if(listofEntityTypes().contains(entityType))
		{
		 rsp= query(QueryStatement,entityType);
		}
		else System.out.println("Can't get_Entity.. Entity Type not exist...");
		return rsp;
		}
//.....
	public Response getEntity(String entityType, int from, int to) {
		Response rsp=new Response();
	if (from==0)
		{
			String QueryStatement="select * from `" +entityType+ "`"+" limit "+to ;
			rsp=query2(QueryStatement,entityType);
	}else{
		String QueryStatement="select count(*) from `" +entityType+ "`"+" limit "+from ;
		SelectRequest selectRequest = new SelectRequest(QueryStatement);
			SelectResult result1 = sdb.select(selectRequest);
			result1.getItems();
			String firstNextoken=result1.getNextToken();

		String QueryStatement2="select * from `" +entityType+ "`"+" limit "+to ;
		//SelectRequest selectRequest2 = new SelectRequest(QueryStatement2);
	  	List<Entities> entities=new ArrayList<Entities>();
	if(listofEntityTypes().contains(entityType))
	{
	 String nextToken = null;
		boolean first=true;
	// do{
		SelectRequest selectRequest2 = new SelectRequest(QueryStatement2);
		if(first){
			selectRequest2.setNextToken(firstNextoken);
			first=false;
        }else if(nextToken!=null)
        	selectRequest2.setNextToken(nextToken);
		
		SelectResult result = sdb.select(selectRequest2);
		//-------- check if no result -----------------------
		if(result.getItems().size()==0){System.out.print("No result match the query"); System.exit(1);;}
		for (Item item : result.getItems()) {
		String entityName=	item.getName();
	    EntityKey entKey=new EntityKey();
		entKey.setStringkey(entityName);
		//--set key and type..
		Entities entity=new Entities();
		entity.setEntityType(entityType);
		entity.setKey(entKey);
		//--set properties
		 for (Attribute attribute : item.getAttributes()) {
			 String attrName=attribute.getName();
			 PropertyValue attrValue=new PropertyValue();
			 attrValue.setString(attribute.getValue());
				Properties property= new Properties();
				property.setProperity(attrName, attrValue);
			 entity.setProperties(property);
	
			 }
			 //add entity to the list
			 entities.add(entity);
			}

	}		else System.out.println("Can't execute query..Entity Type not exist...");
	rsp.setEntities(entities);
}//end else
  	  return rsp;	
		}
	//....
	public Response getEntity(String entityType, List<EntityKey>  keys) {
		String QueryStatement="select * from `" +entityType+ "`" ;
		QueryStatement+= "  where itemName() in ( ";
		for(int keyIndex=0;keyIndex<keys.size();keyIndex++){
			EntityKey k=keys.get(keyIndex);
			if(k.hasIntkey())
				QueryStatement+="'"+k.getIntkey()+"'";
			else  if(k.hasObjectIdkey())
				QueryStatement+="'"+k.getObjectIdkey().toString()+"'";
			else 
				QueryStatement+="'"+k.getStringkey()+"'";
			
			if(keyIndex<keys.size()-1) QueryStatement+=" , ";
		}
		QueryStatement+=" )";
		System.out.println("QueryStatement:"+ QueryStatement);
		Response rsp=new Response();
		//*_Before execute query, check  entityType exist.!!
		if(listofEntityTypes().contains(entityType))
		{
		 rsp= query(QueryStatement,entityType);
		}
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

		// create domain if it is not exist.
		sdb.createDomain(new CreateDomainRequest(entityType));
		//if user dosen't want to replace the entity, 
		//delete the existing one and then add the new one...
		if(ReplaceIfExist==false){
			deleteEntity(entityType,entityKey);
			System.out.println("delete ..");
		}
		 List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();
		 
		 ReplaceableItem item=null;
		 Collection<ReplaceableAttribute> attributes=new  ArrayList<ReplaceableAttribute>() ;
		
		 //----------------------------------------
	        // Add  entity properties:
		 int index=0;
		 while(index<propertyList.size()){
			 ReplaceableAttribute attr=null;
			 
			//***********Check data type*******
			 if(propertyList.get(index).getPropertyValue().hasArrayValue())
			 {
				 int x=0;
				 while(x<propertyList.get(index).getPropertyValue().getArray().length)
				 { PropertyValue [] ValueArray=propertyList.get(index).getPropertyValue().getArray();
				 String value="";
				 if(ValueArray[x].hasbooleanValue())
					value= ValueArray[x].getBoolean()+"";
				 else if(ValueArray[x].hasDateValue())
						value= ValueArray[x].getDate()+"";
				 else if(ValueArray[x].hasDoubleValue())
						value= ValueArray[x].getDouble()+"";
				 else if(ValueArray[x].haslongValue())
						value= ValueArray[x].getLong()+"";
				 else 
						value= ValueArray[x].getString();
				 
				  attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),value,false);
					 attributes.add(attr);
					 	 x++;
				 }
				 
			 }else
			  {String value="";
			 PropertyValue prop= propertyList.get(index).getPropertyValue();
				 if(prop.hasbooleanValue())
					value= prop.getBoolean()+"";
				 else if(prop.hasDateValue())
						value= prop.getDate()+"";
				 else if(prop.hasDoubleValue())
						value= prop.getDouble()+"";
				 else if(prop.haslongValue())
						value= prop.getLong()+"";
				 else 
						value= prop.getString();
				 attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),value,true);
			  attributes.add(attr);
			  	 }
			  index++;
		 }

		 //--add the item--
		 String key="";
		 if (entityKey.hasIntkey() )
			 key=entityKey.getIntkey()+"";
		 else  if (entityKey.hasObjectIdkey() )
			 key=entityKey.getObjectIdkey()+"";
		  else  if(entityKey.haslistkey())
		  { String keys="";
		  for(int i=0;i<entityKey.getListkey().size();i++)
		  keys+=entityKey.getListkey().get(i)+"";
		  key =keys;
		  }	
		 else
			 key=entityKey.getStringkey();
		 item=new ReplaceableItem(key).withAttributes( attributes);
		 sampleData.add( item);       
		sdb.batchPutAttributes(new BatchPutAttributesRequest(entityType, sampleData));
		
	}
	public void put(String entityType,List<Entities> entitiesList) {

		// create domain if it is not exist.
		sdb.createDomain(new CreateDomainRequest(entityType));
		//sdb.createDomain(new CreateDomainRequest(entityType+"_Catalog"));

		//if user dosen't want to replace the entity, 
		//delete the existing one and then add the new one...
	
		 List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();
		// List<ReplaceableItem> catalogSampleData = new ArrayList<ReplaceableItem>();
for(int entityIndex=0;entityIndex<entitiesList.size();entityIndex++)	{ 
		 ReplaceableItem item=null;
		 Collection<ReplaceableAttribute> attributes=new  ArrayList<ReplaceableAttribute>() ;
		 List<Properties> propertyList=entitiesList.get(entityIndex).getProperties();
		 //----------------------------------------
	        // Add  entity properties:
		 int index=0;
		 while(index<propertyList.size()){
			 ReplaceableAttribute attr=null;

			//***********Check data type*******
			 if(propertyList.get(index).getPropertyValue().hasArrayValue())
			 {
				 int x=0;
				 while(x<propertyList.get(index).getPropertyValue().getArray().length)
				 { PropertyValue [] ValueArray=propertyList.get(index).getPropertyValue().getArray();
				 String value="";
				 if(ValueArray[x].hasbooleanValue())
					value= ValueArray[x].getBoolean()+"";
				 else if(ValueArray[x].hasDateValue())
						value= ValueArray[x].getDate()+"";
				 else if(ValueArray[x].hasDoubleValue())
						value= ValueArray[x].getDouble()+"";
				 else if(ValueArray[x].haslongValue())
						value= ValueArray[x].getLong()+"";
				 else 
						value= ValueArray[x].getString();
				 
				  attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),value,false);
					 attributes.add(attr);
				
					 x++;
				 }
				 
			 }else
			  {String value="";
			 PropertyValue prop= propertyList.get(index).getPropertyValue();
				 if(prop.hasbooleanValue())
					value= prop.getBoolean()+"";
				 else if(prop.hasDateValue())
						value= prop.getDate()+"";
				 else if(prop.hasDoubleValue())
						value= prop.getDouble()+"";
				 else if(prop.haslongValue())
						value= prop.getLong()+"";
				 else 
						value= prop.getString();
				 attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),value,true);
			  attributes.add(attr);

				 }
			  index++;
		 }

		 //--add the item--
		 EntityKey entityKey=entitiesList.get(entityIndex).getKey();
		 String key="";
		 if (entityKey.hasIntkey() )
			 key=entityKey.getIntkey()+"";
		 else  if (entityKey.hasObjectIdkey() )
			 key=entityKey.getObjectIdkey()+"";
		  else  if(entityKey.haslistkey())
		  { String keys="";
		  for(int i=0;i<entityKey.getListkey().size();i++)
		  keys+=entityKey.getListkey().get(i)+"";
		  //System.out.println("*******keys:"+keys);
		  key =keys;
		  }	
		 else
			 key=entityKey.getStringkey();
		 item=new ReplaceableItem(key).withAttributes( attributes);
		
		 sampleData.add( item);    
	}//end for
		sdb.batchPutAttributes(new BatchPutAttributesRequest(entityType, sampleData));
		
	}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ) {
		
			DeleteAttributesRequest delete=		new DeleteAttributesRequest(entityType, entityKey.getStringkey());
			sdb.deleteAttributes(delete);
			System.out.println("Delete from.."+entityType+">"+entityKey.getStringkey());
		
	}
	/**
	 * <p>
	 * Delete EntityType. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		//*_Before delete, check if type exist 
		if(listofEntityTypes().contains(entityType)){
		DeleteDomainRequest delete=		new DeleteDomainRequest(entityType);
		sdb.deleteDomain(delete);
		System.out.println("Delete Entity Type called " + entityType + ".\n");
		}
				    
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
		    sdb.createDomain(new CreateDomainRequest(entityType));	

	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){

	ListDomainsResult domains = sdb.listDomains();
	List<String> list=new ArrayList<String>();
	
	for (String s : domains.getDomainNames()) {
		list.add(s);
	}
	return list;
	}


	
}
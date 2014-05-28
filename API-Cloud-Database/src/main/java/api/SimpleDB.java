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
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;



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
				
	SelectRequest selectRequest = new SelectRequest(QueryStatement);

  	List<Entities> entities=new ArrayList<Entities>();
	//-------- check if no result -----------------------
	if(sdb.select(selectRequest).getItems().size()==0){System.out.print("No result match the query"); System.exit(1);;}
	for (Item item : sdb.select(selectRequest).getItems()) {
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
  	  Response rsp=new Response();
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
		GetAttributesRequest greq =new GetAttributesRequest();
		greq.setDomainName(entityType);
		greq.setItemName(entityKey.getStringkey());
		GetAttributesResult gres=	sdb.getAttributes(greq);
	List<Attribute> atrr=	gres.getAttributes();
	int index=0;
	Response rp=new Response();
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
		Response rsp= query(QueryStatement,entityType);
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
		if(!ReplaceIfExist){
			deleteEntity(entityType,entityKey);
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
				  attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),ValueArray[x].getString(),false);
					 attributes.add(attr);
					 x++;
				 }
				 
			 }else
			  {attr= new ReplaceableAttribute(propertyList.get(index).getPropertyName(),propertyList.get(index).getPropertyValue().getString(),true);
			  attributes.add(attr);}
			  //System.out.println(index+"..>"+propertyList.get(index).getPropertyName()+"\t"+propertyList.get(index).getPropertyValue());
			  index++;
		 }
		 //--add the item--
		 item=new ReplaceableItem(entityKey.getStringkey()).withAttributes( attributes);
		 sampleData.add( item);       
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
	 
	}
}


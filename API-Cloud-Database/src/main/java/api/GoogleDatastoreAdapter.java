package main.java.api;

import java.util.List;


/**
* 
*<p>
* GoogleDatastoreAdapter </p>
* 
* @CDport 1.0
* 
*/

public class GoogleDatastoreAdapter extends GoogleDatastore implements Database
{
private String datasetId="";
private GoogleDatastore ds;

	
	public GoogleDatastoreAdapter(){
		super();
		 ds=new GoogleDatastore();

	}
	/**
	 *Constructor.
	 * 
	 * @param datastore id
	 */
	public GoogleDatastoreAdapter(String dsID){
		super();
		datasetId=dsID;
		 ds=new GoogleDatastore();
	}
	
	/**
	 * Connect to Google Cloud Datastore.
	 * 
	 *  @param
	 *  
	 */
	
	public void connect() {
		ds.connectToDatastore(datasetId);}
	
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple select Queries</p>
	 * The retrieved statement will be converted into GQL Query.	 
	 * 
	 * @param QueryStatement array of String, For example: String statement={"select","*","from","entityType","where","field='school'"};
	 * @return  Response 
	 */
	
	public Response query(String [] QueryStatement) {
		String QStatement=QueryArrayToString(QueryStatement);
		Response queryResponse=	ds.query(QStatement);
		return queryResponse;	
	}
	
	
	/**
	 * <p> Get Property.</p>
	 * <p>
	 * Get all properties that belong to the <i> entity type </i>and the <i>Entity Key</i> that are specified.</p>
	 * EntityKey can be <b>long</b> or <b> string </b>.
	 * 
	 * @return Response
	 */
	
	public Response getProperty(String EntityType,EntityKey entityKey) {
		Response resp=	ds.getProperty(EntityType, entityKey);
		return resp;
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
	
	public Response getEntity(String EntityType) {
		Response rsp=ds.getEntity(EntityType);	
		return rsp;
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
	
	public void put(String EntityType,EntityKey entityKey,List<Properties> propertyList ,boolean ReplaceIfExist ) {
		ds.put(EntityType, entityKey, propertyList, ReplaceIfExist );
}
 
	
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	
	public void deleteEntity(String entityType,EntityKey entityKey ){
		ds.deleteEntity(entityType,entityKey);
	}
	//************* To convert array into String ***********
	private String QueryArrayToString (String [] arrayStatment ){
	String  stringStatment="";
	String entityType="";
	int index=1;
	//each array should be at least has 4 index e.g. {"select","*","from","tableName"}
	if(arrayStatment.length>=3&&arrayStatment[0].equalsIgnoreCase("select"))
	{
				stringStatment=stringStatment+arrayStatment[0];
			while(index<arrayStatment.length)
			{	
				//get the entity type
			if(arrayStatment[index].equalsIgnoreCase("from"))
				entityType=arrayStatment[index+1];
			//if current string = KEY, replace it by __key__ and then add it to the new statement String
			if(arrayStatment[index].equals("KEY"))
				stringStatment=stringStatment+" __key__ ";
			//if it is any other string, add it to the new string statement
			else stringStatment=stringStatment+" "+arrayStatment[index];	
			//if current string = where, I need to check if the condition is about the key to replace it by __key__ and then add it to the new statement String
				if(arrayStatment[index].equalsIgnoreCase("where"))
					{//stringStatment=stringStatment+" "+arrayStatment[index];
					//loop to check all index after the where to replace KEY if exist by "KEY"+"("+entityType+","+"'"+ value+"')"
					index++; 
					while(index<arrayStatment.length)
					{	
						if(arrayStatment[index].contains("KEY"))
							{String newCondition=getKeyCondition(arrayStatment[index],entityType);
							stringStatment=stringStatment+" "+newCondition;
							
							}
						else stringStatment=stringStatment+" "+arrayStatment[index];  
						
					index++;}}//end while
			
			index++;	
			}
	}else System.out.print("error");
return stringStatment;
}
	//************* To get the condition parts: name, operator,value ***********
	private String getKeyCondition(String conditionStatment, String entityType){
	String newCondition="";
	String name,value;
	
	String opArray[]={"=","!=",">","<"};
	int count=0;
	 int opIndex=0;
	String operation="";
	 while(count<opArray.length){		  
		 if(conditionStatment.contains(opArray[count]))
			 {opIndex=conditionStatment.indexOf(opArray[count]); 
			 operation=conditionStatment.substring(opIndex, opIndex+1);
			 value=conditionStatment.substring(opIndex+1,conditionStatment.length());
			 
			 name="__key__";
			 value="KEY"+"("+entityType+","+ value+")";
			 newCondition=" "+name+" "+operation+value;		
			 }
		 count++;
	}
		System.out.println(newCondition);
	
	return newCondition;
	}
}

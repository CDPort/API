package main.java.api;

import java.util.List;


/**
* 
*<p>
* SimpleDBAdapter </p>
* 
* @CDport 1.0
* 
*/

public class SimpleDBAdapter extends SimpleDB implements Database
{
	private String secretKey="";
	private String accessKey="";
	private  String region="";
	private SimpleDB sdb;
	private String entityType="";


	public SimpleDBAdapter(){
		super();
		sdb= new SimpleDB();

	}
	/**
	 *Constructor.
	 * 
	 * @param accessKEY
	 * @param secretKEY
	 */
	public SimpleDBAdapter(String accessKEY, String secretKEY,  String regionName ){
		super();
		accessKey=accessKEY;
		secretKey=secretKEY;
	    region= regionName;
		sdb= new SimpleDB();

	}

	/**
	 * Connect to Amazon SimpleDB.
	 * 
	 *  @param
	 *  
	 */
	
	
	public void connect() {
sdb.connectToSimpleDB(accessKey,secretKey,region);

}
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple select Queries</p>
	 *
	 * @param QueryStatement array of String, For example: String statement={"select","*","from","entityType","where","field='school'"};
	 * 
	 * @return  Response 
	 */
	
	public Response query(String [] QueryStatement) {
		String QStatement=QueryArrayToString(QueryStatement);
		Response queryResponse=	sdb.query(QStatement, entityType);
		return queryResponse;	
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
		Response rsp=sdb.getProperty(entityType, entityKey);
		return rsp;
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
	Response rsp=sdb.getEntity(entityType);	
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

	public void put(String EntityType,EntityKey entityKey,List<Properties> propertyList ,boolean ReplaceIfExist ) {
		sdb.put(EntityType, entityKey, propertyList, ReplaceIfExist );
}
 
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ){
		sdb.deleteEntity(entityType,entityKey);
	}
	
	//************* To convert array into String ***********
	private String QueryArrayToString (String [] arrayStatment ){
		String  stringStatment="";
		int index=1;
		//each array should be at least has 4 index e.g. {"select","*","from","tableName"}
		if(arrayStatment.length>=3&&arrayStatment[0].equalsIgnoreCase("select"))
		{
					stringStatment=stringStatment+arrayStatment[0];
				while(index<arrayStatment.length)
				{	
				//if current string = KEY, replace it by itemName() and then add it to the new statement String
				if(arrayStatment[index].equals("KEY"))
					stringStatment=stringStatment+" itemName() ";
				//if it is any other string, add it to the new string statement
				else stringStatment=stringStatment+" "+arrayStatment[index];	
				if(arrayStatment[index].equalsIgnoreCase("from")) entityType=arrayStatment[index+1];
				//if current string = where, I need to check if the condition is about the key to replace it by __key__ and then add it to the new statement String
					if(arrayStatment[index].equalsIgnoreCase("where"))
						{//stringStatment=stringStatment+" "+arrayStatment[index];
						//loop to check all index after the where to replace KEY if exist by "KEY"+"("+entityType+","+"'"+ value+"')"
						index++; 
						while(index<arrayStatment.length)
						{	
							if(arrayStatment[index].contains("KEY"))
								{String newCondition=getKeyCondition(arrayStatment[index]);
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
	private String getKeyCondition(String conditionStatment){
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
			 name="itemName()";			 value=conditionStatment.substring(opIndex+1,conditionStatment.length());
			 
			 newCondition=" "+name+" "+operation+value;		
			 }
		 count++;
	}

	return newCondition;
	}
	}


package main.java.api;

import java.util.List;

import com.mongodb.BasicDBObject;



/**
 * 
 *<p>
 * MongoDBAdapter  </p>
 * 
 * @CDport 1.0
 * 
 */

public class MongoDBAdapter extends MongoDB implements Database
{
	private String MongoClientURI="";
	private MongoDB mdb;
	//---for Query----
	private String entityType="";
	private BasicDBObject newCondition = null;
	private BasicDBObject fields=null;
	
	
	public MongoDBAdapter(){
		super();
		mdb= new MongoDB();

	}
	/**
	 *Constructor.
	 * 
	 * @param mongoClientURI
	 */
	public MongoDBAdapter(String mongoClientURI){
		super();
		MongoClientURI=mongoClientURI;
		
		mdb= new MongoDB();

	}


	/**
	 * Connect to MongoDB.
	 * 
	 *  @param
	 *  
	 */
	
	public void connect() {
	mdb.connectToMongoDB(MongoClientURI);
	
	}
	/**
	 * <p>
	 * Query.</p>
	 * <p> To execute simple select Queries</p>
	 * The query statement will be converted to get the variables that are needed to execute the query by using custom APIs' functions.
	 *
	 * @param QueryStatement array of String, For example: String statement={"select","*","from","entityType","where","field='school'"};
	 * @return  Response 
	 */
	public Response query(String [] QueryStatement) {
		CheckQueryStatment(QueryStatement);
		Response queryResponse=	mdb.query(entityType,newCondition, fields);
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
		Response rsp=mdb.getProperty(entityType, entityKey);
		return rsp;
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
	Response rsp=mdb.getEntity(entityType);	
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
	public void put(String EntityType,EntityKey entityKey,List<Properties> propertyList ,boolean ReplaceIfExist ) {
		mdb.put(EntityType, entityKey, propertyList, ReplaceIfExist );
}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ){
		mdb.deleteEntity(entityType,entityKey);
	}
	
	//*******check the query statement ************
	private void CheckQueryStatment (String [] arrayStatment ){
		int index=1;
		//each array should be at least has 4 index ie select,*,from,tableName
		if(arrayStatment.length>=3&&arrayStatment[0].equalsIgnoreCase("select"))
		{
			while(index<arrayStatment.length)
			{
				if(index==1){
					if(!arrayStatment[1].contains("*")){// check if there are specified fields 
						while(!arrayStatment[index].equalsIgnoreCase("from")){
						//if current string = KEY, replace it by _id 
							if(arrayStatment[index].equals("KEY"))
							{ if(fields!=null) fields.append("_id", true);
							  else fields = new BasicDBObject("_id",true);			
							}
							//if it is any other string
							else {if(fields!=null) fields.append(arrayStatment[index], true);		
									else fields = new BasicDBObject(arrayStatment[index],true); 
								}

						index++;}
					}}
				//get the entity type
				if(arrayStatment[index].equalsIgnoreCase("from"))
					{entityType=arrayStatment[index+1];
					//_____check if there is ' or ` before the name_____________
					entityType=removeSpaces(entityType);
					}
				//if current string = where, I need to check if the condition is about the key to replace it by __key__ and then add it to the new statement String
					if(arrayStatment[index].equalsIgnoreCase("where"))
						checkCondition(arrayStatment, index);
						
				index++;	
				}
		}else System.out.print("error");
	}
//************* To get the condition parts: name, operation,value ***********
	private void checkCondition(String [] arrayStatment,int index){
	do{
		String name,value;
		String opArray[]={"=","!=",">","<"};
		int count=0;
		 int opIndex=0;
		String operation="";
		
		String conditionStatment= arrayStatment[index];
		 while(count<opArray.length){		  
			 if(conditionStatment.contains(opArray[count]))
			{	opIndex=conditionStatment.indexOf(opArray[count]); 
				 operation=conditionStatment.substring(opIndex, opIndex+1);
				 name= conditionStatment.substring(0, opIndex);
				 if(name.contains("KEY")) //<< change the KEY to _id	
					 name="_id";		
				 value=conditionStatment.substring(opIndex+1,conditionStatment.length());
			 	// call the method to set the BasicDBObject	
				setDBObject(name,operation,value);	
	 		}
				 
			 count++;
		}//end while
		 index++;}while(index<arrayStatment.length);
	}
	//********* 
	private void setDBObject (String name,String operation,String value){
		//check if the value  is string or integer
	if(value.contains("'")) 
		{value=removeSpaces(value); //if it is string then there is an need to remove the single quotation at the end and beginning
		if (newCondition==null) //if there is more than one condition, check if it is the first condition
			 {newCondition=new BasicDBObject();
			 	if(operation.equals("=")) newCondition.put(name, value); // << for the = operation 
			 	else newCondition.put(name,new BasicDBObject(operation, value));
			 }
		else {//else if it is not the first condition
			if(operation.equals("="))newCondition.append(name, value);
			else newCondition.append(name, new BasicDBObject(operation, value));
			}
		}
	else{// the value is number not a string
		if (newCondition==null)
		 {newCondition=new BasicDBObject();
		 	if(operation.equals("="))newCondition.put(name,Long.parseLong(value));
		 	else {newCondition.put(name,new BasicDBObject(operation, Long.parseLong(value)));}
		 }
		else {
		if(operation.equals("="))newCondition.append(name, Long.parseLong(value));
		else newCondition.append(name, new BasicDBObject(operation, Long.parseLong(value)));
		}  
		}
	}
	//*********** this method helps to delete the quotation in the string value*********
	private String removeSpaces(String str){
		while(str.startsWith("'")||str.startsWith("`")||str.startsWith(" ")){
			str=str.substring(1);	}
		while(str.endsWith("'")||str.endsWith("`")|| str.endsWith(" ")){
			str=str.substring(0, str.length()-1);	}
			return str;
	}
}






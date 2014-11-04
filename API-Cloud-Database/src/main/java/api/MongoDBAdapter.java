package main.java.api;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
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
		
	Response queryResponse= new Response();
	if(QueryStatement.length>=3&&QueryStatement[0].equalsIgnoreCase("select"))
		{	boolean join=false;
			for(int index=0;index<QueryStatement.length;index++)
			{	if(QueryStatement[index].contains("join")||QueryStatement[index].contains("JOIN"))  {
					queryResponse=selectWithJoin(QueryStatement);
					join=true;
					break;}
			}
			if(join==false)
			{CheckQueryStatment(QueryStatement);
			 queryResponse=mdb.query(entityType,newCondition, fields);
			}
		}
	else if(QueryStatement[0].equalsIgnoreCase("create"))
		{
			createEntityType(QueryStatement[1]);
		}
	//.........delete..........
	else if(QueryStatement[0].equalsIgnoreCase("delete"))
		{ boolean hasCondition=false;
		String EntityT=QueryStatement[1];
		if(QueryStatement[1].equalsIgnoreCase("from"))
			EntityT=QueryStatement[2];
			for(int index=0;index<QueryStatement.length;index++)
			{
				if(QueryStatement[index].contains("where"))
				{
					String prop=QueryStatement[index+1];
					deleteWithCondition(EntityT,prop);
					hasCondition=true;
					break;
				}
			}
			if(hasCondition==false)
			deleteEntityType(EntityT);
		}
	//..........update........
			else if(QueryStatement[0].equalsIgnoreCase("update"))
			{
			String EntityT=QueryStatement[1];
			ArrayList<String>whereStatment=new ArrayList<String>();			
			ArrayList<String> setStatment=new ArrayList<String>();

				for(int index=0;index<QueryStatement.length;index++)
				{//get set conditions "set pop1=v1, prop2=v2,..."
					if(QueryStatement[index].contains("set")){
						int x=index+1; 
						while(x<QueryStatement.length){
							if(QueryStatement[x].equalsIgnoreCase("where")) break;
							setStatment.add(QueryStatement[x]);
							x++; 
						}
					}
				//get properties "where prop1=v1"
					if(QueryStatement[index].equalsIgnoreCase("where"))
					{
						int x=index+1; 
						while(x<QueryStatement.length){
							whereStatment.add(QueryStatement[x]);						
							x++;
						}
					}
				}

				System.out.println("set:"+setStatment.get(0));
				//System.out.println("where:"+whereStatment.get(0));
				updateQuery(EntityT,setStatment,whereStatment);//update Method
			}//...............insert................
			else if (QueryStatement[0].equalsIgnoreCase("insert")){
				String EntityT=QueryStatement[2];//insert into EntityT ( 
				EntityKey k=new EntityKey();
				//--property name-------	
				int x=3; 
				List<String> pNames=new ArrayList<String>();
				while(x<QueryStatement.length && QueryStatement[x].equalsIgnoreCase("values")==false){
					if(QueryStatement[x].contains("(")||QueryStatement[x].contains(")")||QueryStatement[x].equalsIgnoreCase(","))
						;
					else {pNames.add(QueryStatement[x]);
						System.out.println("name:"+QueryStatement[x]);

						}
				x++;		
				}
				//----property values----
				List<PropertyValue> pValues=new ArrayList<PropertyValue>();
				int y=x+1;
				while(y<QueryStatement.length){
					if(QueryStatement[y].contains("(")||QueryStatement[y].contains(")")||QueryStatement[y].equalsIgnoreCase(","))
						;
					else{PropertyValue v= new PropertyValue();
						v.setString(QueryStatement[y]);
						pValues.add(v);
						System.out.println("value:"+QueryStatement[y]);
						}
				y++;		
				}
				//create property list
				List<Properties> propertyList =new ArrayList<Properties>();
				for(int pIndex=0;pIndex<pNames.size();pIndex++)
					{Properties p=new Properties();
					if(pNames.get(pIndex).equalsIgnoreCase("KEY")==false){
						p.setProperity(pNames.get(pIndex), pValues.get(pIndex));
						propertyList.add(p);
						System.out.println(pNames.get(pIndex)+".."+pValues.get(pIndex));
						}
					else 
						k.setStringkey(pValues.get(pIndex).getString());
					}
				put(EntityT,k,propertyList,true);
			}
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
	public Response getEntity(String entityType, int from, int to) {
		Response rsp=mdb.getEntity(entityType, from, to);	
		return rsp;
		}
	public Response getEntity(String entityType, List<EntityKey>keys) {
		Response rsp=mdb.getEntity(entityType,keys);	
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
	public void put(String EntityType,List<Entities> entitiesList  ) {
		mdb.put(EntityType, entitiesList );
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
	/**
	 * <p>
	 * Delete Entity Type. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		mdb.deleteEntityType(entityType);
	}
	/**
	 * <p>
	 * Create EntityType. </p>
	 * 
	 */
	public void createEntityType(String entityType){
		mdb.createEntityType(entityType);
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		return mdb.listofEntityTypes();
		}

		/**
		 * <p>
		 * List of Entity Types_Catalog. </p>
		 * 
		 */
		//public List<String> listofCatalogs(){
	//		return mdb.listofCatalogs();	}
	
	//*****************deleteWithCondition***********
			private void deleteWithCondition(String EntityT,String prop){
				String [] Q={"select","*","from",EntityT,"where",prop};
				CheckQueryStatment(Q);
				//response
				Response resp=mdb.query(entityType,newCondition, fields);
				for (int x=0;x<resp.getEntities().size();x++)
				{
					Entities e=	resp.getEntities().get(x);
					deleteEntity(EntityT,e.getKey());
					System.out.println("Delete entityType >>>>"+EntityT);
				}
			}
			//***************** Update Query ***********
			private void updateQuery(String EntityT,ArrayList<String> setStatment,ArrayList<String> whereStatment){
				//set properties
				Hashtable<String,PropertyValue> prop= new Hashtable<String,PropertyValue>();
				for(int i=0;i<setStatment.size();i++)
				{
					String name=setStatment.get(i).substring(0,setStatment.get(i).indexOf("="));
					String v=setStatment.get(i).substring(setStatment.get(i).indexOf("=")+1,setStatment.get(i).length());
				   
					PropertyValue value=new PropertyValue();
					
					if(v.startsWith("'"))
					    {v=v.substring(1, v.length());
					    if(v.endsWith("'"))v=v.substring(0, v.length()-1);
					    value.setString(v); 
					    }
				    else   value.setLong(Long.parseLong(v));

					
				    prop.put(name, value);
				}
			//retrieve entities
				ArrayList<String> queryArray= new ArrayList<String>();
				queryArray.add("select");
				queryArray.add("*");
				queryArray.add("from");
				queryArray.add(EntityT);
				
				if(whereStatment.size()>0) 
				{queryArray.add("where");
				for(int j=0;j<whereStatment.size();j++)
					{
					queryArray.add(whereStatment.get(j));
					}
				}
				
				//create Query Array 
				String [] Q=new String[queryArray.size()];
				for(int i=0;i<queryArray.size();i++){
				Q[i]=queryArray.get(i);	
				}
				CheckQueryStatment(Q);
				//response
				System.out.println(Q);
				Response resp=mdb.query(entityType,newCondition, fields);
				for (int x=0;x<resp.getEntities().size();x++)
				{ 
					Entities e=	resp.getEntities().get(x);
					List<Properties> newPropList=new ArrayList<Properties>();
					for(int k=0;k<e.getProperties().size();k++){
						String n= e.getProperties().get(k).getPropertyName();
						PropertyValue v=e.getProperties().get(k).getPropertyValue();
						Properties p=new Properties();
						if(!prop.containsKey(n))
							{
							p.setProperity(n, v);
								 System.out.println(k+n+":"+v.getString());
							}
						else //set the updated property value
							{
							p.setProperity(n, prop.get(n));	
							System.out.println(k+prop.get(n).getString()+"<"+n);
							}
						newPropList.add(p);
						
					}//check if there is new properties not exist before
					Iterator<String> s=prop.keySet().iterator();
					while(s.hasNext())
					{	Properties p=new Properties();
					String snext=s.next();

					if(!newPropList.contains(snext)) {
						String newName=snext;
						p.setProperity(newName, prop.get(newName));
						newPropList.add(p);
						System.out.println("l"+newName+","+prop.get(newName).getString());
					}
					}//update
					mdb.put(e.getEntityType(), e.getKey(), newPropList, true);
				}
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
	//*************Join**********************
	private Response selectWithJoin (String [] arrayStatment){
			int index=1;
			String table1="";
			String table2="";
			String condition="";
			String joinType="";
			String option="";
			List<String> col= new ArrayList<String>();
				while(index<arrayStatment.length)
				{	
					
					if(arrayStatment[index].equalsIgnoreCase("from"))
						{table1=arrayStatment[index+1]; //<<table 1
						int c=1;//to start from index 1 (after 'select')
							while(c<index)//get column list
							{
							col.add(arrayStatment[c]);
							c++;
							}
						}
					else if(arrayStatment[index].equalsIgnoreCase("join")) 
						{joinType=arrayStatment[index];
						table2=arrayStatment[index+1];//<<table 2
						}
					else if(arrayStatment[index].equalsIgnoreCase("on")) 
						condition=arrayStatment[index+1]; 
						//index+2;
					else if(arrayStatment[index].contains("option")||arrayStatment[index].contains("OPTION")) 
					option=arrayStatment[index].substring(arrayStatment[index].indexOf("=")+1, arrayStatment[index].length());
					
					//*** check if there is still statm....<<
					//System.out.println(	index + arrayStatment[index]);

					index++;
				}//end while
				//_______________create Q statment____________
				BasicDBObject fields1=null;
				BasicDBObject fields2=null;
				
				//get column
				int x=0;
				while(x<col.size()){
						String c= col.get(x);
						String entityType="";
						String prop="";
						//
					if(c.contains("*")){
						//QueryStam1+=c;
						//QueryStam2+=c;
						//break;
					}
					// if not * >>get entityType name and col.
					else if(c.contains(".")){
						entityType=c.substring(0, c.indexOf("."));
						prop=c.substring(c.indexOf(".")+1, c.length());
						if(prop.equals("KEY")) prop="_id";
						//if it is for entityType1 then add the property to the Q Statement of entityType1.
						if(entityType.equalsIgnoreCase(table1))
						{//add the property
							if(prop.equals("KEY"))
							{ if(fields1!=null) fields1.append("_id", true);
							  else fields1 = new BasicDBObject("_id",true);			
							}
							//if it is any other string
							else {if(fields1!=null) fields1.append(prop, true);		
									else fields1 = new BasicDBObject(prop,true); 
								}
						}
						else if(entityType.equalsIgnoreCase(table2))
							{
							if(prop.equals("KEY"))
							{ if(fields2!=null) fields2.append("_id", true);
							  else fields2 = new BasicDBObject("_id",true);			
							}
							//if it is any other string
							else {if(fields2!=null) fields2.append(prop, true);		
									else fields2 = new BasicDBObject(prop,true); 
								}
							}
					}
					
				x++;}
				
				
				//---get condition---
				String prop1="";
				String prop2="";
				String operation="";
				String t1="";
				String t2="";
				String opArray[]={"=","!=",">","<"};
				int count=0;
				 int opIndex=0;
				 String part1="";
				 String part2="";
				 while(count<opArray.length){		  
					 if(condition.contains(opArray[count]))
						 {opIndex=condition.indexOf(opArray[count]); 
						 
						 operation=condition.substring(opIndex, opIndex+1);//<<operation
						 part1=condition.substring(0,opIndex);
						 part2=condition.substring(opIndex+1,condition.length()); 
						 }
					 count++;
				}
					 
			  if(part1.contains(".")){
					t1=part1.substring(0, part1.indexOf("."));
					if(t1.equals(table1))
					prop1=part1.substring(part1.indexOf(".")+1, part1.length());
					else
					prop2=part1.substring(part1.indexOf(".")+1, part1.length());
			  }
			  if(part2.contains(".")){
				  t2=part2.substring(0, part2.indexOf("."));
				  if(t2.equals(table2))
				prop2=part2.substring(part2.indexOf(".")+1, part2.length());
				else
				prop1=part2.substring(part2.indexOf(".")+1, part2.length());
			  }
			
			  
			//check if prop not on of the col and then add it to the statement
				boolean p1exist=false;
				boolean p2exist=false;

				for(int i=0;i<col.size();i++ ){
					if(col.get(i).contains("*"))
					{p1exist=true; p2exist=true; break;}
					else{if(col.get(i).contains(part1))
							p1exist=true;
						if(col.get(i).contains(part2))
							p2exist=true;
					}
				}
			
				
				if(p1exist==false)
				{if(prop1.equals("KEY")) 
					{if(fields1!=null) fields1.append("_id", true);
					  else fields1 = new BasicDBObject("_id",true);	
					}
				
				else {if(fields1!=null) fields1.append(prop1, true);		
					  else fields1 = new BasicDBObject(prop1,true); 
					}					
				}
			if(p2exist==false)
				{if(prop1.equals("KEY")) 
					{if(fields2!=null) fields2.append("_id", true);
					  else fields2 = new BasicDBObject("_id",true);	
					}
			  
				else
					{if(fields2!=null) fields2.append(prop2, true);		
					else fields2 = new BasicDBObject(prop2,true); 
						}
				}
			
				
				// System.out.println("part1-"+part1+"-part2-"+part2+">>T1"+t1+"t2"+t2+";");

			  //---
			
				//System.out.println("Q stat::: "+table1+">"+fields1.toString()+" _________"+fields2.toString());
				// ___after create statement  -->> execute Query ____
				Response Qresp1=	mdb.query(table1,null, fields1);
				Response Qresp2=	mdb.query(table2,null, fields2);
				
				
				List<Entities> entityList1=Qresp1.getEntities();
				List<Entities> entityList2=Qresp2.getEntities();

				
		
				//--- (outer, inner,col,joinType,condition)
			JoinQuery joinQ= new JoinQuery();
			Response resp=new Response();

			if(option.equalsIgnoreCase("nestedloop"))
			resp=joinQ.JoinQueryNestedLoop(Qresp2,Qresp1,col,joinType,prop1,prop2);
			else if(option.equalsIgnoreCase("mergejoin"))
				{//sort
				entityList1=	selectionSort( entityList1, prop1);	// System.out.println("________"+entityList1.get(0).getEntityType());
				entityList2=	selectionSort( entityList2,prop2);
				
				Qresp1.setEntities(entityList1);
				Qresp2.setEntities(entityList2);
					//join
				resp=joinQ.JoinQueryMerge(Qresp1,Qresp2,col,joinType,prop1,prop2);
				}
				else if(option.equalsIgnoreCase("hashjoin"))
					 resp=joinQ.JoinQueryHash(Qresp1,Qresp2,col,joinType,prop1,prop2);
						
			return resp;
	}
		//---------------------------------
		private List<Entities> selectionSort(List<Entities> entities,String prop){
			  int lenD = entities.size();
			  int j = 0;
			  Entities tmp = new Entities();
					  
			  for(int i=0;i<lenD-1;i++){
			    j = i;
			    for(int k = i+1;k<lenD;k++){
			    	//---------get index of property-----------
					  int index=-1;
							int propindexLength=	entities.get(j).getProperties().size();
								for(int propindex=0;propindex<propindexLength;propindex++)
								{if(entities.get(j).getProperties().get(propindex).getPropertyName().equals(prop))
								index=propindex;
								}

						int index2=-1;
						int propindexLength2=	entities.get(k).getProperties().size();
							for(int propindex=0;propindex<propindexLength2;propindex++)
							{if(entities.get(k).getProperties().get(propindex).getPropertyName().equals(prop))
							index2=propindex;
							}
							if(index==-1||index2==-1) ;
							else{
							String value1="";	String value2="";
							if (entities.get(j).getProperties().get(index).getPropertyValue().haslongValue())
							 {  value1=entities.get(j).getProperties().get(index).getPropertyValue().getLong()+"";
						  	   value2=entities.get(k).getProperties().get(index2).getPropertyValue().getLong()+"";}		
							 else if (entities.get(j).getProperties().get(index).getPropertyValue().hasDateValue())
							 { value1=entities.get(j).getProperties().get(index).getPropertyValue().getDate()+"";
								  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getDate()+"";}		
							  else //***array!!
							  { value1=entities.get(j).getProperties().get(index).getPropertyValue().getString();
								  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getString();}									  

							if(isNumeric(value1)) 
							{System.out.println("-------->int");
								if(Double.parseDouble(value1)>Double.parseDouble(value2)){
						        j = k;
						      }
							}

					  //--------------------------------
							else{ //String
								if(value1.compareTo(value2)>0){
						        j = k;
						      }}//end else String
							}//end else
			    }
			    if(j!=i){// System.out.println("swap"+j+"instead of"+i+"value"+entities.get(j).getProperties().get(0).getPropertyValue().getString());
			    tmp = entities.get(i);
			    entities.set(i,  entities.get(j));
			    entities.set(j,tmp);
			    }
			  }
		  
			  return entities;
			}
		//---------------
		private static boolean isNumeric(String str)  
		{  
		  try  
		  {  
		    double d = Double.parseDouble(str);  
		  }  
		  catch(NumberFormatException nfe)  
		  {  
		    return false;  
		  }  
		  return true;  
		}
	}


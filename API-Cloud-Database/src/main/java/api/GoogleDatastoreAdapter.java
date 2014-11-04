package main.java.api;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
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
	
	public Response query(String [] QueryStatement) {//*****does not check if  entity type exist.....
		Response queryResponse= new Response();
		//.........select..........
		if(QueryStatement[0].equalsIgnoreCase("select"))
		{ boolean join=false;
				for(int index=0;index<QueryStatement.length;index++)
				{	if(QueryStatement[index].contains("join")||QueryStatement[index].contains("JOIN"))
						{queryResponse= selectWithJoin(QueryStatement); 
						join=true;
						break;}
				}
					if(join==false)
					{String QStatement=QueryArrayToString(QueryStatement);
					 queryResponse= ds.query(QStatement);
					}
				
		}//.........create..........
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
	 * EntityKey can be <b>long</b> or <b> string </b>.
	 * 
	 * @return Response
	 */
	
	public Response getProperty(String EntityType,EntityKey entityKey) {
		Response resp=new Response();
		//*_Before execute query, check  entityType exist.!!
		if(listofEntityTypes().contains(EntityType)){
			 resp=	ds.getProperty(EntityType, entityKey);
		}		else System.out.println("Can't get_Properties..Entity Type not exist...");
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
	
	public Response getEntity(String entityType) {
		Response resp=new Response();
				if(listofEntityTypes().contains(entityType))
				{ resp=ds.getEntity(entityType);	
				}else System.out.println("Can't get_Entity.. Entity Type not exist...");

		return resp;
	}
	public Response getEntity(String entityType,int from, int to) {
		Response rsp=ds.getEntity(entityType, from, to);	
		return rsp;
	}
	public Response getEntity(String entityType ,List<EntityKey> keys) {
		Response resp=new Response();
				if(listofEntityTypes().contains(entityType))
				{ resp=ds.getEntity(entityType,keys);	
				}else System.out.println("Can't get_Entity.. Entity Type not exist...");

		return resp;
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
	public void put(String EntityType,List<Entities> entitiesList ) {
		ds.put(EntityType, entitiesList );
	
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
	/**
	 * <p>
	 * Delete Entity Type. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		//*_Before delete, check if type exist 
	if(listofEntityTypes().contains(entityType)){
		ds.deleteEntityType(entityType);
	}
	}
	/**
	 * <p>
	 * Create EntityType. </p>
	 * 
	 */
	public void createEntityType(String entityType){
		ds.createEntityType(entityType);
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		return ds.listofEntityTypes();
		}

		/**
		 * <p>
		 * List of Entity Types_Catalog. </p>
		 * 
		 */

	//*****************deleteWithCondition***********
		private void deleteWithCondition(String EntityT,String prop){
			String Q="select "+"*"+" from "+EntityT+" where ";
			if(prop.contains("KEY"))
				Q+=getKeyCondition(prop,EntityT);
			else Q+=prop;
			//response
			Response resp=ds.query(Q);
			for (int x=0;x<resp.getEntities().size();x++)
			{
				Entities e=	resp.getEntities().get(x);
				deleteEntity(EntityT,e.getKey());
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

				
			  //  System.out.println (i+"name:"+name+"_value:"+v);
			    prop.put(name, value);
			}
		//retrieve entities
			String Q="select "+"*"+" from "+EntityT;
			if(whereStatment.size()>0) Q+=" where ";
			for(int j=0;j<whereStatment.size();j++)
				{if(whereStatment.get(j).contains("KEY"))
					Q+=getKeyCondition(whereStatment.get(j), EntityT);
				else Q+=whereStatment.get(j);
				}
			//response
			//System.out.println(Q);
			Response resp=ds.query(Q); //System.out.println(resp.getEntities().size());
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
						}
					else //set the updated property value
						{
						p.setProperity(n, prop.get(n));	
						}
					newPropList.add(p);
					
				}//check if there is new properties not exist before
				Iterator<String> s=prop.keySet().iterator();
				while(s.hasNext())
				{	Properties p=new Properties();
				String news=s.next();
				if(!newPropList.contains(news)) {
					String newName=news;
					p.setProperity(newName, prop.get(newName));
					newPropList.add(p);
				}
				}//update
				ds.put(e.getEntityType(), e.getKey(), newPropList, true);
			}
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
			{	//if(arrayStatment[index].contains("join")||arrayStatment[index].contains("JOIN"))  {selectWithJoin(arrayStatment); break;}
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
			//	System.out.println(	index + arrayStatment[index]);

				index++;
			}//end while
			//_______________create Q statment____________
			String[ ] QueryStam1=new String [30];
			String  [ ]QueryStam2= new String [30];
			
			QueryStam1[0]="select ";
			QueryStam2[0]="select ";
			int Qindex1=1;
			int Qindex2=1;

			//get column
			int x=0;
			while(x<col.size()){
					String c= col.get(x);
					String entityType="";
					String prop="";
					//
				if(c.contains("*")){
					QueryStam1[Qindex1]=c;Qindex1++;
					QueryStam2[Qindex2]=c;Qindex2++;
					break;
				}
				// if not * >>get entityType name and col.		
				else if(c.contains(".")){
					entityType=c.substring(0, c.indexOf("."));
					prop=c.substring(c.indexOf(".")+1, c.length());
					if(prop.equals("KEY")) prop=" __key__ ";
					//if it is for entityType1 then add the property to the Q Statement of entityType1.
					if(entityType.equalsIgnoreCase(table1))
						{ 					System.out.print(x+prop);

						if(Qindex1>1) QueryStam1[Qindex1]=", "+prop;
						else
						QueryStam1[Qindex1]=prop;//add the property
						
						Qindex1++;}
					else if(entityType.equalsIgnoreCase(table2))
						{ if(Qindex2>1) QueryStam2[Qindex2]=", "+prop;
						else QueryStam2[Qindex2]=prop;
						
						Qindex2++;}
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
				else{
					if(col.get(i).equals(part1))
						p1exist=true;
					if(col.get(i).equals(part2))
						p2exist=true;
				}
			}
				if(p1exist==false)
					{
					if(Qindex1>1) //QueryStam1[Qindex1]=" , ";
						{if(prop1.equals("KEY")) QueryStam1[Qindex1]=" , __key__  ";
						else QueryStam1[Qindex1]=" ," +prop1;
						}
					else 
						{if(prop1.equals("KEY")) QueryStam1[Qindex1]=" __key__ ";
						else QueryStam1[Qindex1]= prop1;
						}
					Qindex1++;}
				if(p2exist==false)
					{if(Qindex2>1) //QueryStam2[Qindex2]=" , ";
						{if(prop2.equals("KEY")) QueryStam2[Qindex2]=" , __key__ ";
						else QueryStam2[Qindex2]=", "+prop2;
						}
					else
					{if(prop2.equals("KEY")) QueryStam2[Qindex2]=" __key__ ";
					else QueryStam2[Qindex2]=prop2;
					}
					Qindex2++;
					}
		
			
				//System.out.println("part1-"+part1+"-part2-"+part2+">>T1"+t1+"t2"+t2+";");

				  //---
					QueryStam1[Qindex1]=" from "+table1;
					QueryStam2[Qindex2]=" from "+table2;
					
					String Q1="";
					String Q2="";
					for(int i=0;i<Qindex1+1;i++)
						Q1+=QueryStam1[i];
					for(int j=0;j<Qindex2+1;j++)
						Q2+=QueryStam2[j];
					
					System.out.println("Q stat::: "+Q1+" _________"+Q2);
					// ___after create statement  -->> execute Query ____
					Response Qresp1=	ds.query(Q1);
					Response Qresp2=	ds.query(Q2);
					
					List<Entities> entityList1=Qresp1.getEntities();
					List<Entities> entityList2=Qresp2.getEntities();

			
					//--- (outer, inner,col,joinType,condition)
				JoinQuery joinQ= new JoinQuery();
				Response resp=new Response();

				if(option.equalsIgnoreCase("nestedloop"))
				resp=joinQ.JoinQueryNestedLoop(Qresp2,Qresp1,col,joinType,prop1,prop2);
				else if(option.equalsIgnoreCase("mergejoin"))
					{//sort
					entityList1=	selectionSort( entityList1, prop1);	 System.out.println("________"+entityList1.get(0).getEntityType());
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
								else if (entities.get(j).getProperties().get(index).getPropertyValue().hasDoubleValue())
								  {  value1=entities.get(j).getProperties().get(index).getPropertyValue().getDouble()+"";
							  	  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getDouble()+"";}										
								else if (entities.get(j).getProperties().get(index).getPropertyValue().hasbooleanValue())
								  {  value1=entities.get(j).getProperties().get(index).getPropertyValue().getBoolean()+"";
								  	  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getBoolean()+"";}		
								 else if (entities.get(j).getProperties().get(index).getPropertyValue().hasDateValue())
								 { value1=entities.get(j).getProperties().get(index).getPropertyValue().getDate()+"";
									  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getDate()+"";}		
								  else //***array!!
								  { value1=entities.get(j).getProperties().get(index).getPropertyValue().getString();
									  value2=entities.get(k).getProperties().get(index2).getPropertyValue().getString();}									  
								if(isNumeric(value1)) 
								{
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


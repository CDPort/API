package main.java.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;


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
		Response queryResponse= new Response();
		//.........select........
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
					 queryResponse= sdb.query(QStatement, entityType);
					}
				
		}//........create..........
		else if(QueryStatement[0].equalsIgnoreCase("create"))
		{
			createEntityType(QueryStatement[1]);
		}//.........delete..........
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

		}//..........update........
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
	 * EntityKey can be <b> String </b> only.
	 * 
	 * @return Response
	 */
	public Response getProperty(String entityType,EntityKey entityKey ) {
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
	public Response getEntity(String entityType,int from, int to) {
		Response rsp=sdb.getEntity(entityType, from, to);	
		return rsp;
	}
	public Response getEntity(String entityType, List<EntityKey>  keys) {
		Response rsp=sdb.getEntity(entityType,keys);	
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
	public void put(String entityType,List<Entities> entitiesList) {
		sdb.put(entityType,entitiesList);
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
	/**
	 * <p>
	 * Delete Entity Type. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		sdb.deleteEntityType(entityType);
	}
	/**
	 * <p>
	 * Create EntityType. </p>
	 * 
	 */
	
	public void createEntityType(String entityType){
		sdb.createEntityType(entityType);
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		return sdb.listofEntityTypes();}


		
	//*****************deleteWithCondition***********
	private void deleteWithCondition(String EntityT,String whereStatment){
		String Q="select "+"*"+" from "+EntityT+" where ";
		if(whereStatment.contains("KEY"))
			Q+=getKeyCondition(whereStatment);
		else Q+=whereStatment;
		//response
		Response resp=sdb.query(Q, EntityT);
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
		    
			if(v.startsWith("'"))v=v.substring(1, v.length());
		    if(v.endsWith("'"))v=v.substring(0, v.length()-1);
		    
		    PropertyValue value=new PropertyValue();
		    value.setString(v);
			
		    prop.put(name, value);
		}
	//retrieve entities
		String Q="select "+"*"+" from "+EntityT;
		if(whereStatment.size()>0) Q+=" where ";
		for(int j=0;j<whereStatment.size();j++)
			{if(whereStatment.get(j).contains("KEY"))
				Q+=getKeyCondition(whereStatment.get(j));
			else Q+=whereStatment.get(j);
			}
		//response
		Response resp=sdb.query(Q, EntityT);
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
			sdb.put(e.getEntityType(), e.getKey(), newPropList, true);
		}
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
				{	//if(arrayStatment[index].contains("join")||arrayStatment[index].contains("JOIN")){ selectWithJoin(arrayStatment);  break;}
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
				else if(arrayStatment[index].equalsIgnoreCase("join")||arrayStatment[index].equalsIgnoreCase("left join")||arrayStatment[index].equalsIgnoreCase("right join")) 
					{joinType=arrayStatment[index];
					table2=arrayStatment[index+1];//<<table 2
					}
				else if(arrayStatment[index].equalsIgnoreCase("on")) 
					condition=arrayStatment[index+1]; 
					//index+2;
				else if(arrayStatment[index].contains("option")||arrayStatment[index].contains("OPTION")) 
				{option=arrayStatment[index].substring(arrayStatment[index].indexOf("=")+1, arrayStatment[index].length());
				//System.out.println("________________________________________________"+option);
				}
				////*** check if there is still statm....<<

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
					if(prop.equals("KEY")) prop="itemName()";
					//if it is for entityType1 then add the property to the Q Statement of entityType1.
					if(entityType.equalsIgnoreCase(table1))
						{ 					//System.out.print(x+prop);

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
						{if(prop1.equals("KEY")) QueryStam1[Qindex1]=" , itemName() ";
						else QueryStam1[Qindex1]=" ," +prop1;
						}
					else 
						{if(prop1.equals("KEY")) QueryStam1[Qindex1]=" itemName() ";
						else QueryStam1[Qindex1]= prop1;
						}
					Qindex1++;}
				if(p2exist==false)
					{if(Qindex2>1) //QueryStam2[Qindex2]=" , ";
						{if(prop2.equals("KEY")) QueryStam2[Qindex2]=" , itemName() ";
						else QueryStam2[Qindex2]=", "+prop2;
						}
					else
					{if(prop2.equals("KEY")) QueryStam2[Qindex2]="itemName() ";
					else QueryStam2[Qindex2]=prop2;
					}
					Qindex2++;
					}
		
			
		  //---
			QueryStam1[Qindex1]=" from "+table1;
			QueryStam2[Qindex2]=" from "+table2;
			
			String Q1="";
			String Q2="";
			for(int i=0;i<Qindex1+1;i++)
				Q1+=QueryStam1[i];
			for(int j=0;j<Qindex2+1;j++)
				Q2+=QueryStam2[j];
			
			// ___after create statement  -->> execute Query ____
			Response Qresp1=	sdb.query(Q1, table1);
			Response Qresp2=	sdb.query(Q2, table2);
			
			List<Entities> entityList1=Qresp1.getEntities();
			List<Entities> entityList2=Qresp2.getEntities();

	
			//--- (outer, inner,col,joinType,condition)
		JoinQuery joinQ= new JoinQuery();
		Response resp=new Response();
		
		if(option.equalsIgnoreCase("nestedloop"))
		resp=joinQ.JoinQueryNestedLoop(Qresp2,Qresp1,col,joinType,prop1,prop2);
		else if(option.equalsIgnoreCase("mergejoin"))
			{//sort
			
			entityList1=	selectionSort( entityList1, prop1);	 
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
						String value1=	entities.get(j).getProperties().get(index).getPropertyValue().getString();
						String value2=	entities.get(k).getProperties().get(index2).getPropertyValue().getString();

						if(isNumeric(value1)) 
						{//System.out.println("-------->int");
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
		    if(j!=i){
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


package main.java.api;

import java.util.ArrayList;
import java.util.List;


/**
* 
*<p>
* AmazonRDSAdapter </p>
* 
* @CDport 1.0
* 
*/

public class AmazonRDSAdapter extends AmazonRDS implements Database
{
	private String secretKey="";
	private String accessKey="";
	private  String region="";
	private  String endpoint="";
	private String jdbcurl="";

	private AmazonRDS rds;
	private String entityType="";


	public AmazonRDSAdapter(){
		super();
		rds= new AmazonRDS();

	}
	/**
	 *Constructor.
	 * 
	 * @param accessKEY
	 * @param secretKEY
	 * @param region
	 * @param endpoint
	 */
	public AmazonRDSAdapter(String accessKEY, String secretKEY,  String regionName, String endPoint,  String jdbcUrl ){
		super();
		accessKey=accessKEY;
		secretKey=secretKEY;
	    region= regionName;
	    endpoint=endPoint;
	    jdbcurl=jdbcUrl;
		rds= new AmazonRDS();

	}

	/**
	 * Connect to AmazonRDS.
	 * 
	 *  @param
	 *  
	 */
	
	
	public void connect() {
rds.connectToAmazonRDS(accessKey,secretKey,region,endpoint,jdbcurl);

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
					 queryResponse= rds.query(QStatement, entityType);
					 System.out.println(".....Query:  "+QStatement);
					}
				
		}
		//........create..........insert...........drop
		else if(QueryStatement[0].equalsIgnoreCase("create")||QueryStatement[0].equalsIgnoreCase("insert")||QueryStatement[0].equalsIgnoreCase("drop"))
		{
			//set entity type
		 entityType=QueryStatement[2];//"insert into entitytype";
			StringBuffer result = new StringBuffer();
		for (int i = 0; i < QueryStatement.length; i++) {
			   result.append( QueryStatement[i] );
			   result.append(" ");
			}
			String QStatement=result.toString();
			queryResponse= rds.query(QStatement, entityType);
		 System.out.println(".....Query:  "+QStatement);
		}//.........delete..........
		else if(QueryStatement[0].equalsIgnoreCase("delete"))
		{ 
		String QStatement=QueryArrayToString(QueryStatement);
		queryResponse= rds.query(QStatement, entityType);
		 System.out.println(".....Query:  "+QStatement);
		}//..........update........
		else if(QueryStatement[0].equalsIgnoreCase("update"))
		{
		String QStatement=updateQuery(QueryStatement);
		queryResponse= rds.query(QStatement, entityType);
		 System.out.println(".....Query:  "+QStatement);
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
		Response rsp=rds.getProperty(entityType, entityKey);
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
		Response rsp=rds.getEntity(entityType);	
	return rsp;
	}
	public Response getEntity(String entityType, int from, int to) {
	Response rsp=rds.getEntity(entityType,from,to);	
	return rsp;
	}
	public Response getEntity(String entityType, List<EntityKey> keys) {
			Response rsp=rds.getEntity(entityType,keys);	
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
		rds.put(EntityType, entityKey, propertyList, ReplaceIfExist );
}
	public void put(String entityType,List<Entities> entitiesList) {
		rds.put(entityType,entitiesList);
}
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey ){
		rds.deleteEntity(entityType,entityKey);
	}
	/**
	 * <p>
	 * Delete Entity Type. </p>
	 * 
	 */
	public void deleteEntityType(String entityType) {
		rds.deleteEntityType(entityType);
	}
	/**
	 * <p>
	 * Create EntityType. </p>
	 * 
	 */
	
	public void createEntityType(String entityType){
		rds.createEntityType(entityType);
	}
	/**
	 * <p>
	 * List of Entity Types. </p>
	 * 
	 */
	public List<String> listofEntityTypes(){
		return rds.listofEntityTypes();
		}

		/**
		 * <p>
		 * List of Entity Types_Catalog. </p>
		 * 
		 */
		public List<String> listofCatalogs(){
			return rds.listofCatalogs();
		}

	//***************** Update Query ***********
	private String updateQuery(String [] arrayStatment){
		String  stringStatment="";
		int index=3;//index0=update,index1=table,index2=set
		//each array should be at least has 4 index e.g. {"update","table","set","col.value","where","condition}
		if(arrayStatment.length>=3&&arrayStatment[0].equalsIgnoreCase("UPDATE"))
		{
				stringStatment=stringStatment+arrayStatment[0];
				entityType=arrayStatment[1];
				stringStatment+=" "+entityType;
				stringStatment+=" set ";
				while(index<arrayStatment.length)
				{
				//if current string = where, I need to check if the condition is about the key to replace it by __key__ and then add it to the new statement String
				//if current string = KEY, replace it by name of the primary key and then add it to the new statement String
					if(arrayStatment[index].contains("KEY"))
					{String newCondition=getKeyCondition(arrayStatment[index]);
					stringStatment=stringStatment+" "+newCondition;
					
					}
				else stringStatment=stringStatment+" "+arrayStatment[index]; 
					
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
							
						index++;
						if(index<arrayStatment.length) stringStatment+=" , ";
							}}//end while
				
				index++;	
				if(index<arrayStatment.length && arrayStatment[index].equalsIgnoreCase("where")==false) stringStatment+=" , ";
				}
		}else System.out.print("error");
		return stringStatment;

	}

	//************* To convert array into String ***********
	private String QueryArrayToString (String [] arrayStatment ){
		String  stringStatment="";
		int index=1;
		//each array should be at least has 4 index e.g. {"select","*","from","tableName"}
		//if(arrayStatment.length>=3&&arrayStatment[0].equalsIgnoreCase("select"))
		//{
					stringStatment=stringStatment+arrayStatment[0];
					int i=0;
				while(i<arrayStatment.length)
				{		if(arrayStatment[i].equalsIgnoreCase("from")){ entityType=arrayStatment[i+1]; break;}
				i++;}
				while(index<arrayStatment.length)
				{
				//if current string = where, I need to check if the condition is about the key to replace it by __key__ and then add it to the new statement String
				//if current string = KEY, replace it by name of the primary key and then add it to the new statement String
				if(arrayStatment[index].equals("KEY"))
					{List<String> primarykey=rds.getPrimaryKeyName(entityType);
					if(primarykey.size()>1)
						for(int j=0;j<primarykey.size();j++)
					{stringStatment+=" "+primarykey.get(j)+" ";///*** primary key name ***
					if(j<primarykey.size()-1) stringStatment+=" ,";
					}
					else
					stringStatment+=" "+primarykey.get(0)+" ";///*** primary key name ***
					
					}
				//if it is any other string, add it to the new string statement
				else stringStatment=stringStatment+" "+arrayStatment[index];		
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
		//}else System.out.print("error");
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
			 name=rds.getPrimaryKeyName(entityType).get(0);///*** primary key name ***
			 value=conditionStatment.substring(opIndex+1,conditionStatment.length());
			 
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
				//*** check if there is still statm....<<
				//System.out.println(	index + arrayStatment[index]);

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
			
			//System.out.println("Q stat::: "+Q1+" _________"+Q2);
			// ___after create statement  -->> execute Query ____
			Response Qresp1=	rds.query(Q1, table1);
			Response Qresp2=	rds.query(Q2, table2);
			
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

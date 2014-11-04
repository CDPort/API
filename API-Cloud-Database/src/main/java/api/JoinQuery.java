package main.java.api;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

public class JoinQuery {

	
	public Response JoinQueryNestedLoop(Response resp1, Response resp2, List<String> col,String joinType,String prop1,String prop2){	
		  List<Entities> entityList= new ArrayList<Entities>();
		  List<EntityKey> entity1Key=new ArrayList<EntityKey>();
		  List<EntityKey> entity2Key=new ArrayList<EntityKey>();


		 //---------nested loop------------
		  for(Entities entity1:resp1.getEntities()){//for each entity in type1 (outer)
			  for(Entities entity2:resp2.getEntities()){//for each entity in type2 (inner)
				  String p1="";
					 String p2="";

					if(prop1.equals("KEY"))
						 {if(entity1.getKey().hasStringkey())
							 p1=entity1.getKey().getStringkey();
						 else if(entity1.getKey().hasIntkey())
						     p1=entity1.getKey().getIntkey()+"";
						 else if(entity1.getKey().hasObjectIdkey())
							 p1=entity1.getKey().getObjectIdkey()+"";
						 
					
						if(prop2.equals("KEY"))
						 {if(entity2.getKey().hasStringkey())
							 p2=entity2.getKey().getStringkey();
						 else if(entity2.getKey().hasIntkey())
						     p2=entity2.getKey().getIntkey()+"";
						 else if(entity2.getKey().hasObjectIdkey())
							 p2=entity2.getKey().getObjectIdkey()+"";
						 }
					}
					else
					{// to know the index of property and get property value
					   p1=getPropValue(entity1 ,  prop1);
					   p2=getPropValue(entity2,  prop2); 
					}
				  
				  List<Properties> propList= new ArrayList<Properties>();
				  Entities newEntity =new Entities();
				  //if e1 join e2
				//add properties if only match 
				  if(!p1.equals("")&&!p2.equals("")&&p1.equals(p2))
				  { newEntity=  CheckJoin( entity1, entity2,  col,  prop1, prop2);
				  entityList.add(newEntity);
				  entity1Key.add(entity1.getKey());//add key to the list
				  entity2Key.add(entity2.getKey());//add key to the list

				  }
				
			  }//for2

		  }//for1
		//check for the type of join
		  
		   if(joinType.equalsIgnoreCase("RIGHT JOIN")||joinType.equalsIgnoreCase("RIGHT OUTER JOIN"))
			 { 
						  //add all properties of entity1
			   for(Entities entity1:resp1.getEntities()){
					  if(entity1Key.contains(entity1.getKey())==false)//do not add entity that added before to the list
					  { if(entity1.getProperties().isEmpty()==false)//do not add entities that do not have properties
						  {System.out.println("s>"+entity1.getKey().getStringkey());
						  entityList.add( CheckJoin( entity1, new Entities(),  col,  prop1, prop2));
						  }
					  }
			   }
			  
			 }
		  else  if(joinType.equalsIgnoreCase("LEFT JOIN")||joinType.equalsIgnoreCase("LEFT OUTER JOIN"))
			 { 				
			  for(Entities entity2:resp2.getEntities()){
				  if(entity2Key.contains(entity2.getKey())==false)//do not add entity that added before to the list
				  { if(entity2.getProperties().isEmpty()==false)//do not add entities that do not have properties
					  {System.out.println("s>"+entity2.getKey().getStringkey());
					  entityList.add( CheckJoin(new Entities(),entity2,  col,  prop1, prop2));
					  }
				  }
			  }
			 }
		   
		  Response resp=new Response();
		  resp.setEntities(entityList);
		
			return resp;
	}
	//-------------------------------------------------------------------
	private String getPropValue(Entities entity, String prop){
		int index=0;
		String p="";
		while(index<entity.getProperties().size()){
			  if(entity.getProperties().get(index).getPropertyName().equals(prop))
			  { //prop1index=index1;
				  if(entity.getProperties().get(index).getPropertyValue().hasStringValue())
			  		p=entity.getProperties().get(index).getPropertyValue().getString(); 
				  else if (entity.getProperties().get(index).getPropertyValue().haslongValue())
				  		p=entity.getProperties().get(index).getPropertyValue().getLong()+""; 
				  else if (entity.getProperties().get(index).getPropertyValue().hasDoubleValue())
				  		p=entity.getProperties().get(index).getPropertyValue().getDouble()+""; 
				  else if (entity.getProperties().get(index).getPropertyValue().hasbooleanValue())
				  		p=entity.getProperties().get(index).getPropertyValue().getBoolean()+""; 
				  else if (entity.getProperties().get(index).getPropertyValue().hasDateValue())
				  		p=entity.getProperties().get(index).getPropertyValue().getDate()+""; 
				  else if (entity.getProperties().get(index).getPropertyValue().hasArrayValue())//***
				  		p=entity.getProperties().get(index).getPropertyValue().getArray()+""; 
					  
				  break;
			  }
			  index++;
		  }
		return p;
	}
	//------------------------------
private Entities CheckJoin(Entities entity1,Entities entity2, List<String> col, String prop1,String prop2){
	  Entities newEntity =new Entities();
	 
	  if(col.get(0).equals("*"))
	  {  int y=0;
	//>> key
		newEntity.setProperties(addKeyProperty( entity1));
		newEntity.setProperties(addKeyProperty( entity2));
		
	  while(y<entity1.getProperties().size())
	  {
		  Properties p=new Properties();
		
		  if(entity1.getProperties().get(y).getPropertyName().equals(prop1)&&entity1.getProperties().get(y).getPropertyName().equals(prop2))
		         p.setProperity(entity1.getProperties().get(y).getPropertyName(),entity1.getProperties().get(y).getPropertyValue());
		  else 	p.setProperity((entity1.getEntityType()+"."+entity1.getProperties().get(y).getPropertyName()),entity1.getProperties().get(y).getPropertyValue());
		  newEntity.setProperties(p);
	
		  y++;
	  }
	  int x=0;
	  while(x<entity2.getProperties().size())
	  {
		  if(!entity2.getProperties().get(x).getPropertyName().equals(prop1)&&!entity2.getProperties().get(x).getPropertyName().equals(prop2))
		  {
		  Properties p=new Properties();
	  	  p.setProperity((entity2.getEntityType()+"."+entity2.getProperties().get(x).getPropertyName()),entity2.getProperties().get(x).getPropertyValue());
	      newEntity.setProperties(p);}
		  
		  x++;
		  }
	  }
	  else{
	int i=0;
	  while(i<col.size()){//loop col
		  int y=0;
		  while(y<entity1.getProperties().size())
		  {
			  //check if key
			  if(entity1.getEntityType().equals(col.get(i).substring(0, col.get(i).indexOf(".")))&& col.get(i).substring(col.get(i).indexOf(".")+1, col.get(i).length()).equals("KEY"))
				newEntity.setProperties(addKeyProperty( entity1));

		  //if name is the same in condation and in the col. so do not add the tableName.col because it should be =, so add the col one time only.
			  else if(entity1.getEntityType().equals(col.get(i).substring(0, col.get(i).indexOf(".")))&&entity1.getProperties().get(y).getPropertyName().equals(col.get(i).substring(col.get(i).indexOf(".")+1, col.get(i).length())))
				  {Properties p=new Properties();
				  if(entity1.getProperties().get(y).getPropertyName().equals(prop1)&&entity1.getProperties().get(y).getPropertyName().equals(prop2))
				         p.setProperity(entity1.getProperties().get(y).getPropertyName(),entity1.getProperties().get(y).getPropertyValue());
				  else 	p.setProperity((entity1.getEntityType()+"."+entity1.getProperties().get(y).getPropertyName()),entity1.getProperties().get(y).getPropertyValue());
				  newEntity.setProperties(p);
					  }
			 
		  y++;}
		  int x=0;
		  while(x<entity2.getProperties().size())
		  {//check if key
			  if(entity2.getEntityType().equals(col.get(i).substring(0, col.get(i).indexOf(".")))&& col.get(i).substring(col.get(i).indexOf(".")+1, col.get(i).length()).equals("KEY"))
				newEntity.setProperties(addKeyProperty( entity2));

			  else if(entity2.getEntityType().equals(col.get(i).substring(0, col.get(i).indexOf(".")))&&entity2.getProperties().get(x).getPropertyName().equals(col.get(i).substring(col.get(i).indexOf(".")+1, col.get(i).length())))
			  {	

					  Properties p=new Properties();
				  	  p.setProperity((entity2.getEntityType()+"."+entity2.getProperties().get(x).getPropertyName()),entity2.getProperties().get(x).getPropertyValue());
				      newEntity.setProperties(p);
				
			  }
		  x++;}
		  
	 i++; }
	  }
	  return newEntity;
}
//set key
private Properties addKeyProperty(Entities entity1)
	{
	Properties p=new Properties();
	PropertyValue pValue=new PropertyValue();
	if(entity1.getKey().hasIntkey()) pValue.setLong(entity1.getKey().getIntkey());
	else  if(entity1.getKey().hasObjectIdkey()) pValue.setString(entity1.getKey().getObjectIdkey()+"");
	else pValue.setString(entity1.getKey().getStringkey());
	p.setProperity(entity1.getEntityType()+".KEY",pValue);
	return p;
	}
//Sort-Merge join
public Response JoinQueryMerge(Response resp1, Response resp2, List<String> col,String joinType,String prop1,String prop2)
{	 List<Entities> entityList= new ArrayList<Entities>();
	//get first row R1 from input 1
	Entities entity1=new Entities();
	//get first row R2 from input 2
	Entities entity2=new Entities();
	
	 List<EntityKey> entity1Key=new ArrayList<EntityKey>();
	  List<EntityKey> entity2Key=new ArrayList<EntityKey>();

//while not at the end of either input
	int index1=0;
	int index2=0;
	while(index1<resp1.getEntities().size()&&index2<resp2.getEntities().size())
	{			
		entity1=resp1.getEntities().get(index1);
		entity2= resp2.getEntities().get(index2);
	// if R1 joins with R2>> return (R1, R2)
		 String p1="";
		 String p2="";

		if(prop1.equals("KEY"))
			 {if(entity1.getKey().hasStringkey())
				 p1=entity1.getKey().getStringkey();
			 else if(entity1.getKey().hasIntkey())
			     p1=entity1.getKey().getIntkey()+"";
			 else if(entity1.getKey().hasObjectIdkey())
				 p1=entity1.getKey().getObjectIdkey()+"";
			 
		
			if(prop2.equals("KEY"))
			 {if(entity2.getKey().hasStringkey())
				 p2=entity2.getKey().getStringkey();
			 else if(entity2.getKey().hasIntkey())
			     p2=entity2.getKey().getIntkey()+"";
			 else if(entity2.getKey().hasObjectIdkey())
				 p2=entity2.getKey().getObjectIdkey()+"";
			 }
		}
		else
		{// to know the index of property and get property value
		   p1=getPropValue(entity1 ,  prop1);
		   p2=getPropValue(entity2,  prop2); 
		} 
		  List<Properties> propList= new ArrayList<Properties>();
		  Entities newEntity =new Entities();
		  if(!p1.equals("")&&!p2.equals("")&&p1.equals(p2))
		  {  //get property from both entity1 and entity2  			  
			 System.out.println(">>>"+joinType+">>>"+p1+":..:"+p2 +"index2"+index2);
			
			  newEntity= CheckJoin( entity1, entity2,  col,  prop1, prop2);

			  entityList.add(newEntity);
			  entity1Key.add(entity1.getKey());
			  entity2Key.add(entity2.getKey());
			  // get next row R2 from input 2
			  index2++;
		  }  
		  else if(p1.compareTo(p2)<0)
			  index1++; 
		  else 
			  index2++;  

	}//end while
	
	//check for the type of join
	   if(joinType.equalsIgnoreCase("LEFT JOIN")||joinType.equalsIgnoreCase("LEFT OUTER JOIN"))
		 { 
					  //add all properties of entity1
		   for(Entities e1:resp1.getEntities()){
				  if(entity1Key.contains(e1.getKey())==false)//do not add entity that added before to the list
				  { if(e1.getProperties().isEmpty()==false)//do not add entities that do not have properties
					  {System.out.println("s>"+e1.getKey().getStringkey());
					  entityList.add( CheckJoin( e1, new Entities(),  col,  prop1, prop2));
					  }
				  }
		   }
		  
		 }
	  else  if(joinType.equalsIgnoreCase("RIGHT JOIN")||joinType.equalsIgnoreCase("RIGHT OUTER JOIN"))
		 { 				
		  for(Entities e2:resp2.getEntities()){
			  if(entity2Key.contains(e2.getKey())==false)//do not add entity that added before to the list
			  { if(e2.getProperties().isEmpty()==false)//do not add entities that do not have properties
				  {System.out.println("s>"+e2.getKey().getStringkey());
				  entityList.add( CheckJoin(new Entities(),e2,  col,  prop1, prop2));
				  }
			  }
		  }
		 }
	 Response resp=new Response();
	  resp.setEntities(entityList);
	
	return resp;

}

//**********************************Hash join************************************
public Response JoinQueryHash(Response resp1, Response resp2, List<String> col,String joinType,String prop1,String prop2)
{
	List<Entities> entityList= new ArrayList<Entities>();
	
	List<EntityKey> entity1Key=new ArrayList<EntityKey>();
	List<EntityKey> entity2Key=new ArrayList<EntityKey>();
	Hashtable<Integer, LinkedList<Entities>> h=new Hashtable(resp1.getEntities().size());
	
	for(int index=0;index<resp1.getEntities().size();index++){
		Entities entity=resp1.getEntities().get(index);
		LinkedList<Entities>l=  new LinkedList<Entities>();

		String p1="";
		if(prop1.equals("KEY"))
		 {if(entity.getKey().hasStringkey())
			 p1=entity.getKey().getStringkey();
		 else if(entity.getKey().hasIntkey())
		     p1=entity.getKey().getIntkey()+"";
		 else if(entity.getKey().hasObjectIdkey())
			 p1=entity.getKey().getObjectIdkey()+"";
		 }
		 else  p1=getPropValue(entity ,  prop1);
		
	if(!p1.equals(""))
		{	

		int hash1=p1.hashCode();
		//put
		 if(h.contains(hash1))
			{ LinkedList<Entities> existL= h.get(hash1);
			 existL.add(entity);
			 h.put(hash1, l);
			 }	
		 else	
			 {
				l.add(entity);
				h.put(hash1, l);
			 }		 
	
		}
	}

	for(int index2=0;index2<resp2.getEntities().size();index2++){
		Entities entity2=resp2.getEntities().get(index2);
		
		String p2="";
		if(prop2.equals("KEY"))
		 {if(entity2.getKey().hasStringkey())
			 p2=entity2.getKey().getStringkey();
		 else if(entity2.getKey().hasIntkey())
		     p2=entity2.getKey().getIntkey()+"";
		 else if(entity2.getKey().hasObjectIdkey())
			 p2=entity2.getKey().getObjectIdkey()+"";
		 }
		else p2=getPropValue(entity2 ,  prop2);
		
		if(!p2.equals("")){
		 int hash2=p2.hashCode();
			if(h.get(hash2)!=null) 
				{
				LinkedList<Entities> l= h.get(hash2);
				 int x=0;
				while(x<l.size())
					{
					Entities currentEntity=l.get(x);
					 String  CurrentProp=getPropValue(currentEntity ,  prop2);
					 	if(CurrentProp.equals(p2))//find
						 {
							Entities  newEntity= CheckJoin( currentEntity, entity2,  col,  prop1, prop2);
							entityList.add(newEntity);
							 entity1Key.add(currentEntity.getKey());//add key to the list
							  entity2Key.add(entity2.getKey());//add key to the list
						 }
					 x++;
					}
				}
		}
		
		
	}
	//check for the type of join
	   if(joinType.equalsIgnoreCase("LEFT JOIN")||joinType.equalsIgnoreCase("LEFT OUTER JOIN"))
		 { 
					  //add all properties of entity1
		   for(Entities e1:resp1.getEntities()){
				  if(entity1Key.contains(e1.getKey())==false)//do not add entity that added before to the list
				  { if(e1.getProperties().isEmpty()==false)//do not add entities that do not have properties
					  {System.out.println("s>"+e1.getKey().getStringkey());
					  entityList.add( CheckJoin( e1, new Entities(),  col,  prop1, prop2));
					  }
				  }
		   }
		  
		 }
	  else  if(joinType.equalsIgnoreCase("RIGHT JOIN")||joinType.equalsIgnoreCase("RIGHT OUTER JOIN"))
		 { 				
		  for(Entities e2:resp2.getEntities()){
			  if(entity2Key.contains(e2.getKey())==false)//do not add entity that added before to the list
			  { if(e2.getProperties().isEmpty()==false)//do not add entities that do not have properties
				  {System.out.println("s>"+e2.getKey().getStringkey());
				  entityList.add( CheckJoin(new Entities(),e2,  col,  prop1, prop2));
				  }
			  }
		  }
		 }
	 Response resp=new Response();
	  resp.setEntities(entityList);

	return resp;

}

}

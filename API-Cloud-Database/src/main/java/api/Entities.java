package main.java.api;
import java.util.ArrayList;
import java.util.List;


/**
* 
*<p>
* Entities class to set or get entity Key, Entity Type and Property List </p>
* 
* @CDport 1.0
* 
*/

public class Entities
{
	
	private String entitytype;
	private List<Properties> propertyList;
	private EntityKey entitykey;
	
	
	/**
	 * Constructor.
	 * 
	 *  @param
	 *  
	 */
	
	public Entities(){
		super();
		propertyList=new ArrayList<Properties>();
		entitykey=new EntityKey();
	}


	/**
	 * setEntityType.
	 * 
	 *  
	 */
	public void setEntityType(String entityType) {//&&
		entitytype= entityType;	}
		
	/**
	 * setKey.
	 * 
	 *  
	 */
	
	public void setKey(EntityKey key) {//&&
entitykey=key;	}
	
	/**
	 * setProperties.
	 * 
	 *  
	 */
	public void setProperties(Properties property){//&&
		propertyList.add(property);
	}
	/**
	 * getKey.
	 * 
	 *  @return  entity key.
	 */
	public EntityKey getKey() {
return entitykey;	}
	
	/**
	 * getEntityType.
	 * 
	 *  @return  entity type.
	 */
	
	public String getEntityType() {
return entitytype;	}
	
	/**
	 * getProperties.
	 * 
	 *  @return  List of properties
	 */
	public List<Properties> getProperties(){
		return propertyList; 
	}
	
	

}


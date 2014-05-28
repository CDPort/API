package main.java.api;

import java.util.ArrayList;
import java.util.List;


/**
* 
*<p>
* Response class to return the entities and properties</p>
* 
* @CDport 1.0
* 
*/
public class Response {
	List<Entities> entitiesList;
	List<Properties> propertiesList;
	/**
	 * Constructor.
	 * 
	 *  @param
	 *  
	 */
	public Response(){
		super();
		entitiesList=new ArrayList<Entities>();
		propertiesList=new ArrayList<Properties>();
	}

	/**
	 * setEntities.
	 * 
	 *  @param entities List of Entities
	 *  
	 */
	public void setEntities(List<Entities> entities) {
		entitiesList=entities;
		}
	/**
	 * setProperties.
	 * 
	 *  @param properties List of properties
	 *  
	 */
	public void setProperties(List<Properties> properties) {
		propertiesList=properties;}
	/**
	 * getEntities.
	 * 
	 *  @return  List of Entities
	 */
	public List<Entities> getEntities() {
	return entitiesList;
	}
	/**
	 * getProperties.
	 * 
	 *  @return  List of properties
	 *  
	 */
	public List<Properties> getProperties() {
		return propertiesList;}
	

	
}

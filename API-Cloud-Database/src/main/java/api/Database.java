package main.java.api;

import java.util.List;



/**
* 
*<p>
* Database class consists the following Functions: </p>
* <p><i>
* Connect, Query, getProperty,getEntity, put, deleteEntity</p></i>
* 
* @CDport 1.0
* 
*/
public  interface Database 
{
	/**
	 * Connect.
	 * 
	 *  
	 *  
	 */
	public void connect() ;
	
	/**
	 * <p>
	 * Query.</p>
	 * 
	 * @param QueryStatement array of String, For example: String statement={"select","*","from","entityType","where","field='school'"};
.
	 * 
	 * @return  Response 
	 */
	
	public Response query(String [] QueryStatement) ;
	
	/**
	 * <p> Get Property.</p>
	 * <p>
	 * Get all properties that belong to the <i> entity type </i>and the <i>Entity Key</i> that are specified.</p>
	 * @param EntityType String
	 * @param EntityKey each database supports different data types for the key.   
	 * 
	 * @return Response
	 */
	
	
	public Response getProperty(String EntityType,EntityKey entityKey) ;
	
	/**
	 * <p> Get Entity.</p>
	 * <p>
	 * Get all Entities that belong to the specified <i> Entity type </i>.</p>
	 * 
	 * @param EntityType String
	 * @param EntityKey each database supports different data types for the key.   
	 * * 
	 * @return Response
	 * 
	 */
	
	public Response getEntity(String EntityType) ;
	/**
	 * <p> Put.</p>
	 * <p>
	 * To add new Entity. The method also supports replace entity if exist. </p>
	 * 
	 * @param EntityType String
	 * @param EntityKey each database supports different data types for the key.   
	 * @param propertyList The list of properties of the new entity.Each database supports different data types for the property value.
	 * @param ReplaceIfExist (boolean variable). Set it to <b> true </b> if you want to replace the entity if it exists. 
	 *
	 */
	public void put(String EntityType,EntityKey entityKey,List<Properties> propertyList,boolean ReplaceIfExist ) ;
	
	/**
	 * <p>
	 * Delete Entity. </p>
	 * Delete the entity that belong to the <i> entity Type</i> and has the specified <i>Entity Key<i/>.
	 * 
	 * @param EntityType String
	 * @param EntityKey each database supports different data types for the key.   
	 * 
	 */
	public void deleteEntity(String entityType,EntityKey entityKey );
	
}


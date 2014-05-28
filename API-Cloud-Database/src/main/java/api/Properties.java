package main.java.api;

/**
* 
*<p>
* Properties class to set or get property name and value </p>
* 
* @CDport 1.0
* 
*/

public class Properties
{
	private String propertyName;
	private PropertyValue propertyValue;
	
	/**
	 * Constructor.
	 * 
	 *  @param
	 *  
	 */
	public Properties(){
		super();
	}
	
	/**
	 * setProperties.
	 * 
	 * @param name : string
	 * @param value : PropertyValue
	 *  
	 */
	public void setProperity (String name, PropertyValue value){
		propertyName=name;
		propertyValue=value;
	}
	/**
	 * getPropertyName.
	 * 
	 *  @return  name.
	 */
	public String getPropertyName()
	{return propertyName;}
	/**
	 * getPropertyValue.
	 * 
	 *  @return  <b>value</b> as PropertyValue object.
	 */
	public PropertyValue getPropertyValue()
	{return propertyValue;}
	/*public Map<String,PropertyValue> getProperity (){
		Map<String,PropertyValue>	map= new HashMap<String,PropertyValue>();
				 map.put(propertyName,propertyValue);
				 return map;		 
	}*/
}


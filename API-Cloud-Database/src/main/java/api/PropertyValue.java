package main.java.api;

import java.util.Date;

/**
* 
*<p>
* PropertyValue class supports the following data types: <i> String, long, double, boolean, Date and array of PropertyValue. </i> </p>
* 
* @CDport 1.0
* 
*/
public class PropertyValue {
private String stringValue;
private long longValue;
private double doubleValue;
private boolean boolValue;
private Date dateValue;
private PropertyValue [] propertyValueArray;

boolean stringType=false;
boolean longType=false;
boolean doubleType=false;
boolean booleanType=false;
boolean DateType=false;
boolean arrayType=false;
//-----set------
public void setString(String value){	
	stringType=true;
	stringValue=value;}
public void setLong(long value){
	longType=true;
	longValue=value;}
public void setDouble(Double value){
	doubleType=true;
	doubleValue=value;}
public void setBoolean(Boolean value){
	booleanType=true;
	boolValue=value;}
public void setDate(Date date){	
	DateType=true;
	dateValue=date;}
public void setArray(PropertyValue [] propertyValue){
	arrayType=true;
	propertyValueArray=propertyValue;}
//-------get------
public String getString(){	
	return stringValue;}
public long getLong(){	
	return longValue;}
public Double getDouble(){	
	return doubleValue;}
public Boolean getBoolean(){	
	return boolValue;}
public Date getDate( ){	
	return dateValue;}
public PropertyValue [] getArray(){	
	return propertyValueArray;}
//-------check type-----
public boolean hasStringValue(){
	return  stringType;
}
public boolean haslongValue(){
	return  longType;
}public boolean hasDoubleValue(){
	return  doubleType;
}
public boolean hasbooleanValue(){
	return  booleanType;
}
public boolean hasDateValue(){
	return  DateType;
}
public boolean hasArrayValue(){
	return  arrayType;
}

}

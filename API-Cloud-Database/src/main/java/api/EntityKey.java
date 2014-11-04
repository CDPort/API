package main.java.api;

import java.util.List;

import org.bson.types.ObjectId;

/**
* 
*<p>
* EntityKey class supports the following data types: <i> String, long, objectId. </i> </p>
* 
* @CDport 1.0
* 
*/
public class EntityKey {
	
	private String strKey;
	private long intKey;
	private ObjectId objIdK;
	private List<Long> listint;
    boolean stringType=false;
	boolean intType=false;
	boolean objectIdType=false;
	boolean listofint=false;


	public EntityKey(){
		strKey=null;
		intKey=0;
		objIdK=new ObjectId();
	}
	//---set--
		public void setStringkey(String stringKey){
			stringType=true;
			strKey=	stringKey;
		}
		public void setIntkey(long IntKey){
			intType=true;
			intKey=IntKey;
		}
		public void  setObjectIdkey (ObjectId objectIdKey){
			objectIdType=true;
			objIdK= objectIdKey;
		}
		public void  setListkey (List<Long> lint){
			listofint=true;
			listint= lint;
		}
	//---get--
	public String getStringkey(){
		return  strKey;
	}
	public long getIntkey(){
		return  intKey;
	}
	public ObjectId getObjectIdkey(){
		return  objIdK;
	}
	public List<Long>   getListkey ( ){
		return listint;
	}
	//---- check type-----
	public boolean hasStringkey(){
		return  stringType;
	}
	public boolean hasIntkey(){
		return  intType;
	}
	public boolean hasObjectIdkey(){
		return  objectIdType;
	}
	public boolean haslistkey(){
		return  listofint;
	}
}

package com.ibm.mdm.bean;

import java.io.Serializable;
import java.lang.reflect.Method;

public abstract class BaseBean implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String toString()
	{
		StringBuffer buff = new StringBuffer();
		try
		{
			Method[] arr = this.getClass().getMethods();
			for ( int i = 0; i < arr.length; i++ )
			{
				String name = arr[i].getName();
				if ( (name.startsWith("get") || name.startsWith("is")) && arr[i].getParameterTypes().length == 0 )
				{
					String fieldName = getFieldName(name);
					Object obj = arr[i].invoke(this, null);
					buff.append(fieldName);
					buff.append("=");
					buff.append(obj);
					buff.append("\n");
				}
			}
		}
		catch ( Throwable t )
		{
			
		}
		return buff.toString();
	}
	
	private  String getFieldName(String str)
	{
		String ret = null;
		if ( str.startsWith("get") )
		{
			ret = str.substring(3);
		}
		else
		{
			ret = str.substring(2);
		}
		char first = Character.toLowerCase(ret.charAt(0));
		return first + ret.substring(1);
	}
	

}

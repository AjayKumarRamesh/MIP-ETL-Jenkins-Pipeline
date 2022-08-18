package com.ibm.mdm.bean;

public class ColumnView extends BaseBean{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String name;
	
	private String tableName;
	
	private String desc;
	
	private String ifNull;
	
	private String dataType;
	
	private String schema;

	private String dbFlag;

	private String tableObjid;

	public String getTableObjid() {
		return tableObjid;
	}

	public void setTableObjid(String tableObjid) {
		this.tableObjid = tableObjid;
	}

	public String getDbFlag() {
		return dbFlag;
	}

	public void setDbFlag(String dbFlag) {
		this.dbFlag = dbFlag;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getIfNull() {
		return ifNull;
	}

	public void setIfNull(String ifNull) {
		this.ifNull = ifNull;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	

}

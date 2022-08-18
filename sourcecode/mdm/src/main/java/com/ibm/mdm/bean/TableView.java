package com.ibm.mdm.bean;

public class TableView extends BaseBean{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String name;
	
	private String desc;
	
	private String schema;
	
	private String freshRate;
	
	private String schemaDesc;

	private String dbFlag;

	private String schemaId;

	private String tableObjid;

	public String getTableObjid() {
		return tableObjid;
	}

	public void setTableObjid(String tableObjid) {
		this.tableObjid = tableObjid;
	}

	public String getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(String schemaId) {
		this.schemaId = schemaId;
	}

	public String getDbFlag() {
		return dbFlag;
	}

	public void setDbFlag(String dbFlag) {
		this.dbFlag = dbFlag;
	}

	public String getSchemaDesc() {
		return schemaDesc;
	}

	public void setSchemaDesc(String schemaDesc) {
		this.schemaDesc = schemaDesc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getFreshRate() {
		return freshRate;
	}

	public void setFreshRate(String freshRate) {
		this.freshRate = freshRate;
	}

}

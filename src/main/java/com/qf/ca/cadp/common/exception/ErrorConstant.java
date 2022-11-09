package com.qf.ca.cadp.common.exception;

public class ErrorConstant {
	public static final String META_JOB_NOT_EXIST = "META_JOB_NOT_EXIST";
	public static final String META_JOB_ALREADY_PUBLISHED = "META_JOB_ALREADY_PUBLISHED";
	public static final String META_JOB_ALREADY_PAUSED = "META_JOB_ALREADY_PAUSED";
	public static final String META_JOB_IS_ACTIVE = "META_JOB_IS_ACTIVE";
	public static final String META_JOB_ALREADY_EXIST = "META_JOB_ALREADY_EXIST";
	public static final String JOB_IS_ACTIVE = "JOB_IS_ACTIVE";
	public static final String MANDATORY_FIELDS_NOT_SET = "MANDATORY_FIELDS_NOT_SET";
	public static final String ERROR_SAVING_JAR_FILE = "ERROR_SAVING_JAR_FILE";
	public static final String ERROR_PARSING_CONF_FILE = "ERROR_PARSING_CONF_FILE";
	public static final String JOB_CUSTOMIZE_NOT_ALLOWED = "JOB_CUSTOMIZE_NOT_ALLOWED";
	public static final String JOB_NOT_EXIST = "JOB_NOT_EXIST";
	public static final String META_JOB_WRONG_STATUS = "META_JOB_WRONG_STATUS";
	public static final String JOB_ALREADY_EXIST = "JOB_ALREADY_EXIST";
	public static final String META_JOB_ALREADY_UPDATED = "META_JOB_ALREADY_UPDATED";
	public static final String AGG_INDEX_NOT_EXIST = "AGG_INDEX_NOT_EXIST";		//聚合指标不存在
	public static final String IATA_ALREADY_EXIST = "IATA_ALREADY_EXIST";		//租户航司编号二字码已经存在

	public static final String CHINESENAME_ALREADY_EXIST = "CHINESENAME_ALREADY_EXIST"; //航司中文名已存在
	public static final String ENGLISHNAME_ALREADY_EXIST = "ENGLISHNAME_ALREADY_EXIST"; //航司英文名已存在
	public static final String ICAO_ALREADY_EXIST = "ICAO_ALREADY_EXIST";            //航司三字码已存在


	public static final String TENANT_NOT_EXIST = "TENANT_NOT_EXIST";   //租户不存在
	
	public static final String GENERAL_EXCEPTION = "GENERAL_EXCEPTION";        //其他错误
	public static final String GENERAL_EXCEPTION_DESC = "Technical Error";

	public static final String GROUP_NOT_ALLOWED = "GROUP_NOT_ALLOWED";		//分页查询的时候不允许有group 语句
	public static final String GROUP_NOT_ALLOWED_DESC = "Group is not allow in page query";
	
	public static final String CONSTRAINT_EXCEPTION = "CONSTRAINT_EXCEPTION";//数据库唯一约束错误
	public static final String CONSTRAINT_EXCEPTION_DESC = "Constraint Error";

	public static final String MAIL_ALREADY_EXIST = "mail is already exist";
	public static final String PHONE_ALREADY_EXIST = "phone is already exist";
	public static final String USERNAME_ALREADY_EXIST = "username is already exist";
	public static final String ROLE_USER_RELATION_EXIST = "There are associations between users and roles";
	public static final String LIMITED_AUTHORITY = "LIMITED_AUTHORITY";   //权限不足

}

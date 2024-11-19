CREATE OR REPLACE WAREHOUSE WH_TEST;
USE WAREHOUSE WH_TEST;

create or replace database DB_TEST;
USE DATABASE DB_TEST;

create or replace schema SC_TEST;
USE SCHEMA SC_TEST;

create or replace TABLE DB_TEST.SC_TEST.SUCCESS_LOG_TABLE (
	EXECUTION_TIME TIMESTAMP_NTZ(9),
	SP_NAME VARCHAR(200),
	USER_NAME VARCHAR(1000),
	OBJECT_TYPE VARCHAR(1000),
	DDL VARCHAR(16777216)
);

create or replace TABLE DB_TEST.SC_TEST.ERROR_LOG_TABLE (
	ERROR_MSG VARCHAR(16777216),
	ERROR_STATE VARCHAR(100),
	ERROR_STACKTRACETXT VARCHAR(100),
	FAILED_DATE TIMESTAMP_LTZ(9),
	SP_NAME VARCHAR(16777216),
	ERROR_STEP VARCHAR(16777216)
);

create or replace TABLE DB_TEST.SC_TEST.TEMP (
	CREATED_ON TIMESTAMP_LTZ(6),
	MODIFIED_ON TIMESTAMP_LTZ(6),
	PRIVILEGE VARCHAR(16777216),
	GRANTED_ON VARCHAR(16777216),
	NAME VARCHAR(16777216),
	TABLE_CATALOG VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	GRANTED_TO VARCHAR(4),
	GRANTEE_NAME VARCHAR(16777216),
	GRANT_OPTION BOOLEAN,
	GRANTED_BY VARCHAR(16777216),
	DELETED_ON TIMESTAMP_LTZ(6)
);

create or replace TABLE DB_TEST.SC_TEST.STG_GRANTS_TO_ROLE (
	CREATED_ON TIMESTAMP_LTZ(6),
	MODIFIED_ON TIMESTAMP_LTZ(6),
	PRIVILEGE VARCHAR(16777216),
	GRANTED_ON VARCHAR(16777216),
	NAME VARCHAR(16777216),
	TABLE_CATALOG VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	GRANTED_TO VARCHAR(4),
	GRANTEE_NAME VARCHAR(16777216),
	GRANT_OPTION BOOLEAN,
	GRANTED_BY VARCHAR(16777216),
	DELETED_ON TIMESTAMP_LTZ(6)
);

INSERT INTO DB_TEST.SC_TEST.STG_GRANTS_TO_ROLE
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE GRANTEE_NAME!='ACCOUNTADMIN';


create or replace view DB_TEST.SC_TEST.VW_ROLE_HR_BKP(
	RANK,
	LABEL1,
	LABEL2,
	GRANTEE_NAME
) as 
SELECT * FROM
(SELECT DISTINCT 1 AS RANK, GRANTEE_NAME LABEL1,PRIVILEGE LABEL2,GRANTEE_NAME FROM  temp WHERE DELETED_ON IS NULL
 AND GRANTED_ON='WAREHOUSE' OR (GRANTED_ON='ACCOUNT' AND PRIVILEGE LIKE '%WAREHOUSE%')
UNION ALL
SELECT DISTINCT 2 AS RANK, PRIVILEGE LABEL1, GRANTED_ON ||' '||NAME LABEL2,GRANTEE_NAME FROM  temp WHERE DELETED_ON IS NULL
 AND GRANTED_ON='WAREHOUSE'
UNION ALL
SELECT DISTINCT 3 AS RANK, PRIVILEGE LABEL1, GRANTED_ON ||' '||NAME LABEL2,GRANTEE_NAME FROM  temp WHERE DELETED_ON IS NULL
 AND GRANTED_ON='DATABASE' OR (GRANTED_ON='ACCOUNT' AND PRIVILEGE LIKE '%DATABASE%')
UNION ALL
SELECT DISTINCT 4 AS RANK, 'DATABASE'||' '||TABLE_CATALOG LABEL1, 'SCHEMA'||' '||NAME LABEL2,GRANTEE_NAME FROM  temp WHERE DELETED_ON IS NULL
 AND GRANTED_ON='SCHEMA'
UNION ALL
SELECT DISTINCT 5 AS RANK, 'SCHEMA'||' '||NAME LABEL1,PRIVILEGE LABEL2,GRANTEE_NAME FROM TEMP WHERE DELETED_ON IS NULL
 AND GRANTED_ON='SCHEMA') ORDER BY RANK;
 
create or replace view DB_TEST.SC_TEST.VW_ROLE_HR_BKP_FINAL(
	RANK,
	LABEL1,
	LABEL2,
	GRANTEE_NAME
) as
SELECT * FROM VW_ROLE_HR_BKP WHERE (LABEL1,LABEL2) IN (
SELECT LABEL1,LABEL2 FROM VW_ROLE_HR_BKP  
MINUS
(
SELECT LABEL1,LABEL2 FROM VW_ROLE_HR_BKP WHERE LABEL2  IN 
(select LABEL2 from VW_ROLE_HR_bkp where label1='OWNERSHIP' )
AND LABEL1<>'OWNERSHIP'
UNION ALL
SELECT LABEL1,LABEL2 FROM VW_ROLE_HR_BKP WHERE LABEL1  IN 
(select LABEL1 from VW_ROLE_HR_bkp where label2='OWNERSHIP')
AND LABEL2<>'OWNERSHIP'));


CREATE OR REPLACE FUNCTION DB_TEST.SC_TEST.GET_ROLE_TEMP("ROLE_NAME" VARCHAR(16777216))
RETURNS TABLE ("R_NAME" VARCHAR(16777216))
LANGUAGE SQL
AS $$ select distinct name from temp
where grantee_name=ROLE_NAME AND granted_on='ROLE' $$;

CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_ROLE_DB("OBJECT_TYPE" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var lv_step=''
try{

lv_step='Step 00-SP_CREATE_ROLE_DB';

var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);


lv_step='Step 10-SP_CREATE_ROLE_DB';

var cmd1=`CREATE `+OBJECT_TYPE+` `+OBJECT_NAME+``;

snowflake.createStatement({sqlText: cmd1}).execute();

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_CREATE_ROLE_DB','`+res1+`','`+OBJECT_TYPE+`','`+cmd1+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return OBJECT_TYPE+' '+OBJECT_NAME+' Created Successfully';
}

catch(err){

snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_CREATE_ROLE_DB',lv_step]});
return 'Failed: '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_SCHEMA("DB_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

var lv_step='';
try{
lv_step='Step 00- SP_CREATE_SCHEMA';
var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);

lv_step='Step 10- SP_CREATE_SCHEMA';
var cmd1=`CREATE schema `+DB_NAME+`.`+SCHEMA_NAME+``;
snowflake.createStatement({sqlText: cmd1}).execute();

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_CREATE_SCHEMA','`+res1+`','SCHEMA','`+cmd1+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return 'Schema '+SCHEMA_NAME+' Created Successfully';
}

catch(err){
snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_CREATE_SCHEMA',lv_step]});
return 'Failed: '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_TABLE("DB_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "TABLE_NAME" VARCHAR(16777216), "TEST" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var lv_step='';
try{
lv_step='Step 00 - SP_CREATE_TABLE';
var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);

lv_step='Step 10 - SP_CREATE_TABLE';

var cmd=`create table `+DB_NAME+`.`+SCHEMA_NAME+`.`+TABLE_NAME+` (`;
for (let i = 0; i < TEST.length; i++) {
for (let j = 0; j < TEST[i].length; j++) {

cmd=cmd+TEST[i][j]+' ';

}
cmd=cmd+',';
}

var tab=cmd.slice(0, -1)+')';
snowflake.createStatement({sqlText:tab}).execute();

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_CREATE_TABLE','`+res1+`','TABLE','`+tab+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return 'Table '+TABLE_NAME+' Created Successfully';
}
catch(err){

snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_CREATE_TABLE',lv_step]});
return 'Failed: '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_USER_OBJECT("ROLE_NM" VARCHAR(16777216), "WH" VARCHAR(16777216), "USERNAME" VARCHAR(16777216), "PASSWORD" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

var lv_step='';
try{

lv_step='Step 00-SP_CREATE_USER_OBJECT';
var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);

lv_step='Step 10-SP_CREATE_USER_OBJECT';

var cmd1=`CREATE USER `+USERNAME+` password='`+PASSWORD+`' DEFAULT_ROLE='`+ROLE_NM+`'
            DEFAULT_WAREHOUSE='`+WH+`'`;
snowflake.createStatement({sqlText: cmd1}).execute();

var cmd2=cmd1.replaceAll("'","''");

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_CREATE_USER_OBJECT','`+res1+`','USER','`+cmd2+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return 'User Created Successfully';
}

catch(err){
snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_CREATE_USER_OBJECT',lv_step]});
return 'Failed: '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_WAREHOUSE("WH_NAME" VARCHAR(16777216), "WH_SIZE" VARCHAR(16777216), "SUSPEND" FLOAT, "RESUME" VARCHAR(16777216), "MIN" FLOAT, "MAX" FLOAT, "POLICY" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
try{
var cmd1=`CREATE WAREHOUSE `+WH_NAME+` WITH WAREHOUSE_SIZE ='`+WH_SIZE+`'
AUTO_SUSPEND = `+SUSPEND+`
AUTO_RESUME = `+RESUME+`
MIN_CLUSTER_COUNT = `+MIN+`
MAX_CLUSTER_COUNT = `+MAX+`
SCALING_POLICY ='`+POLICY+`'
`;
snowflake.createStatement({sqlText: cmd1}).execute();

return 'Warehouse '+WH_NAME+' Created Successfully';
}

catch(err){
return err.message+' '+err.stackTraceTxt;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_CREATE_WAREHOUSE("WH_NAME" VARCHAR(16777216), "WH_SIZE" VARCHAR(16777216), "SUSPEND" VARCHAR(16777216), "RESUME" VARCHAR(16777216), "MAX" VARCHAR(16777216), "MIN" VARCHAR(16777216), "POLICY" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var lv_step='';
try{

lv_step='Step 00- SP_CREATE_WAREHOUSE';
var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);

lv_step='Step 10- SP_CREATE_WAREHOUSE';

var cmd1=`CREATE WAREHOUSE `+WH_NAME+` WITH WAREHOUSE_SIZE ='`+WH_SIZE+`'
AUTO_SUSPEND = `+SUSPEND+`
AUTO_RESUME = `+RESUME+`
MIN_CLUSTER_COUNT = `+MIN+`
MAX_CLUSTER_COUNT = `+MAX+`
SCALING_POLICY ='`+POLICY+`'
`;
snowflake.createStatement({sqlText: cmd1}).execute();

var cmd2=cmd1.replaceAll("'","''");

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_CREATE_WAREHOUSE','`+res1+`','WAREHOUSE','`+cmd2+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return 'Warehouse '+WH_NAME+' Created Successfully';
}

catch(err){
snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_CREATE_WAREHOUSE',lv_step]});
return 'Failed: '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_EXECUTE_QUERY("QUERY" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var lv_step='';
try{
lv_step='Step 00 - SP_EXECUTE_QUERY';

var user=`select current_user`;

var res_user=snowflake.createStatement({sqlText: user}).execute();
res_user.next();
res1=res_user.getColumnValue(1);

lv_step='Step 10 - SP_EXECUTE_QUERY';

snowflake.createStatement({sqlText: QUERY}).execute();

var cmd2=QUERY.replaceAll("'","''");

var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
values(current_timestamp(),'SP_EXECUTE_QUERY','`+res1+`','File_Upload_Query_Execution','`+cmd2+`')`;

snowflake.createStatement({sqlText: sql}).execute();

return 'SUCCESS ';
}

catch(err){
snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,binds: [err.message,err.state,err.stackTraceTxt,'SP_EXECUTE_QUERY',lv_step]});
return 'Failed '+err.message;
}
$$;
CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_GET_DDL("OBJ_TYPE" VARCHAR(16777216), "SRC_DB" VARCHAR(16777216), "SRC_SCHMA" VARCHAR(16777216), "SRC_OBJ" VARCHAR(16777216), "DEST_DB" VARCHAR(16777216), "DEST_SCHMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

try{
	
	var lv_step=''

	var user=`select current_user`;

	var res_user=snowflake.createStatement({sqlText: user}).execute();
	res_user.next();
	res1=res_user.getColumnValue(1);

	if(OBJ_TYPE==='TABLE'){
	
		lv_step='STEP00-SP_GET_DDL';
		
		var src_ddl=`SELECT GET_DDL('TABLE','`+SRC_DB+`.`+SRC_SCHMA+`.`+SRC_OBJ+`')`;
		var res=snowflake.createStatement({sqlText:src_ddl}).execute();
		res.next();
		var query=res.getColumnValue(1);
		var query1='create or replace TABLE '+DEST_DB+`.`+DEST_SCHMA+'.';
		query=query.replaceAll('create or replace TABLE ',query1);

		snowflake.createStatement({sqlText:query}).execute();
		query=query.replaceAll("'","''");
		var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
		values(current_timestamp(),'SP_GET_DDL','`+res1+`','TABLE_DEPLOY','`+query+`')`;

		snowflake.createStatement({sqlText: sql}).execute();
	}
	else{
	
		lv_step='STEP00-SP_GET_DDL';
		
		var sql_cmd3=`DESC PROCEDURE `+SRC_DB+`.`+SRC_SCHMA+`.`+SRC_OBJ;
		var sql_cmda=snowflake.createStatement({sqlText:sql_cmd3});
		result1=sql_cmda.execute();
		test=[] 
		while (result1.next()){
		temp=[]
		temp.push(result1.getColumnValue(1))
		temp.push(result1.getColumnValue(2))
		test.push(temp)
		}
		var ind_start= SRC_OBJ.indexOf("(");
		var ind_end= SRC_OBJ.indexOf(")");
		var proc_name = SRC_OBJ.substr(0, ind_start) + SRC_OBJ.substr(ind_end+1);

		var str='create or replace procedure '+DEST_DB+'.'+DEST_SCHMA+'.'+proc_name+test[0][1]+ '\n'+test[1][0]+' '+test[1][1]+ '\n'+
		test[2][0]+' '+test[2][1]+ '\n'+
		test[5][0]+' '+test[5][1]+ '\n as \$\$ '+ '\n'+
		test[6][1]+'\n \$\$;';

		snowflake.createStatement({sqlText:str}).execute();
		str=str.replaceAll("'","''");
		var sql=`insert into SUCCESS_LOG_TABLE(EXECUTION_TIME,SP_NAME,USER_NAME,OBJECT_TYPE,DDL)
		values(current_timestamp(),'SP_GET_DDL','`+res1+`','PROCEDURE_DEPLOY','`+str+`')`;

		snowflake.createStatement({sqlText: sql}).execute();
	}

return OBJ_TYPE+' Created Successfully';
}

catch(err){
	snowflake.execute({sqlText: `insert into ERROR_LOG_TABLE VALUES (?,?,?,current_timestamp(),?,?)`,
	binds: [err.message,err.state,err.stackTraceTxt,'SP_GET_DDL',lv_step]});
	return 'Failed ';
}
$$;

CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_LOAD_ROLE("ROLE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

sql_cmd=`insert into temp select * from STG_GRANTS_TO_ROLE where grantee_name='`+ROLE_NAME+`'`;

 snowflake.execute (
            {sqlText: sql_cmd}
            );         
 
 
sql_cmd=`select distinct r_name from table(get_role_temp('`+ROLE_NAME+`'))`;

 res=snowflake.execute (
            {sqlText: sql_cmd}
            );    
 while(res.next())
{
sql_cmd=` call sp_load_role('`+res.getColumnValue(1)+`')`;

 snowflake.execute (
            {sqlText: sql_cmd}
            );         

}            
$$;

CREATE OR REPLACE PROCEDURE DB_TEST.SC_TEST.SP_UPDATE_TEMP_ROLE("ROLE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

sql_cmd=`update temp set grantee_name='`+ROLE_NAME+`'`;

 snowflake.execute (
            {sqlText: sql_cmd}
            );         
 $$;
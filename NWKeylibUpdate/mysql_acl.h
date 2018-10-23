#pragma once
#include "acl_cpp/lib_acl.hpp"

#define _MYSQL_SELECT 1 //select语句
#define _MYSQL_INSERT 2 //insert语句
#define _MYSQL_DELETE 3 //delete语句
#define _MYSQL_UPDATA 4 //updata语句

#ifdef _DEBUG
#pragma comment( lib, "lib_acl_cpp_vc2010d.lib" )
#pragma comment( lib, "lib_acl_vc2010d.lib" )
#else
#pragma comment( lib, "lib_acl_cpp_vc2010.lib" )
#pragma comment( lib, "lib_acl_vc2010.lib" )
#endif



class CMysql_acl
{
public:
	CMysql_acl(void);
	~CMysql_acl(void);

	/************************************************* 
	@Input : 
	[sIP]:    服务器地址
	[nPort]:  服务器端口
	[dbName]: 数据库名
	[sUser]:  用户名
	[sPwd]:   密码
	[charset]:程序使用的字符集;utf8、gb2312、GBK等.
	@Output:  NULL
	@Return:  返回<0失败;select语句返回记录数;其他返回影响行数
	@Others: 
	*************************************************/  
	bool        mysql_connectDB(const char* sIP, int nPort, const char* dbName, const char* sUser, const char* sPwd, const char* charset);
	void        mysql_disconnectDB();

	/************************************************* 
	@Input : 
	[nSqlType]: sql语句类型, 查看宏定义
    [sql]: 执行语句,支持变参
	@Output: NULL
	@Return: 返回<0失败;select语句返回记录数;其他返回影响行数
	@Others:
	*************************************************/  
	int         mysql_exec(int nSqlType, const char* sql, ...);
	

	/************************************************* 
	@Input : 
	[colName]: 列名
	@Output: NULL
	@Return: 一行数据指定列的值
	@Others: 循环调用mysql_getNextRow()获取每一行数据
	*************************************************/  
	bool        mysql_getNextRow();
	int         mysql_getRowIntValue(const char* colName);
	double      mysql_getRowDoubleValue(const char* colName);
	const char* mysql_getRowStringValue(const char* colName);

private:
	bool        mysql_Init();
	int         mysql_Insert(acl::db_handle& h_db, const char* sql);
	int         mysql_Delete(acl::db_handle& h_db, const char* sql);
	int         mysql_Updata(acl::db_handle& h_db, const char* sql);
	int         mysql_Select(acl::db_handle& h_db, const char* sql);

	bool        mysql_getNextRow_private(acl::db_handle& h_db);
	int         mysql_getRowIntValue_private(acl::db_handle& h_db, const char* colName);
	double      mysql_getRowDoubleValue_private(acl::db_handle& h_db, const char* colName);
	const char* mysql_getRowStringValue_private(acl::db_handle& h_db, const char* colName);
	

private:
	acl::db_mysql*     m_pdb;
	size_t             m_nCurRow;  //查询到的数据的某一行数
	size_t             m_nRowCount;//查询到的数据的总行数

};
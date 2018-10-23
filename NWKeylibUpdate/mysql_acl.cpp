#include "stdafx.h"
#include "mysql_acl.h"
#include "LogRecorder.h"

extern CLogRecorder g_LogRecorder;
CMysql_acl::CMysql_acl(void)
{
	m_pdb = NULL;
}

CMysql_acl::~CMysql_acl(void)
{
	delete m_pdb;
	m_pdb = NULL;
}

/********************* 对外方法 ********************/
bool CMysql_acl::mysql_connectDB(const char* sIP, int nPort, const char* dbName, const char* sUser, const char* sPwd, const char* charset)
{
	if (!mysql_Init())
	{
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "****Error: !mysql_Init.");
		return false;
	}

	//连接mysql地址是 127.0.0.1:3306 的形式
	char mysqlAddr[40] = {0}; 
	sprintf_s(mysqlAddr, "%s:%d", sIP, nPort);

	m_pdb = new acl::db_mysql(mysqlAddr, dbName, sUser, sPwd, 0UL,true, 60, 60, charset);

	if (NULL == m_pdb)
	{
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "****Error: NULL.");
		return false;
	}

	if ( !m_pdb->open() )
	{
        string sError = m_pdb->get_error();
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "****Error: %s.", sError.c_str());
		return false;
	}

	return true;
}

void CMysql_acl::mysql_disconnectDB()
{
	if (m_pdb != NULL)
	{
		delete m_pdb;
		m_pdb = NULL;
	}
}

int CMysql_acl::mysql_exec(int nSqlType, const char* sqlFormat, ...)
{
	int nRet = -1;

	va_list arlist;
	va_start(arlist, sqlFormat);
	char sqlConetent[1024 * 4] = {0};
	_vsnprintf_s(sqlConetent, sizeof(sqlConetent), sqlFormat, arlist);
	va_end(arlist);

	switch (nSqlType)
	{
	case _MYSQL_SELECT:
		{
			nRet = mysql_Select(*m_pdb, sqlConetent);
		}
		break;
	case _MYSQL_INSERT:
	case _MYSQL_DELETE:
	case _MYSQL_UPDATA:
		{
			nRet = mysql_Updata(*m_pdb, sqlConetent);
		}
		break;
	default:
		break;
	}

	return nRet;
}

bool CMysql_acl::mysql_getNextRow()
{
	return mysql_getNextRow_private(*m_pdb);
}

int  CMysql_acl::mysql_getRowIntValue(const char* colName)
{
	return mysql_getRowIntValue_private(*m_pdb, colName);
}

double CMysql_acl::mysql_getRowDoubleValue(const char* colName)
{
	return mysql_getRowDoubleValue_private(*m_pdb, colName);
}

const char* CMysql_acl::mysql_getRowStringValue(const char* colName)
{
	return mysql_getRowStringValue_private(*m_pdb, colName);
}

/******************** 内部方法 ********************/
bool CMysql_acl::mysql_Init()
{
	//acl采用动态调用mysql客户端库的方式操作mysql
	//判断程序目录下是否有libmysql.dll文件
	//调试过程注意工作目录
    DWORD nBufferLenth = MAX_PATH;
    char szBuffer[MAX_PATH] = { 0 };
    DWORD dwRet = GetModuleFileNameA(NULL, szBuffer, nBufferLenth);
    char *sPath = strrchr(szBuffer, '\\');
    memset(sPath, 0, strlen(sPath));
    string sCurrentPath = szBuffer;

    sCurrentPath += "/libmysql.dll";


	HANDLE hFile = ::CreateFile(sCurrentPath.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (hFile == INVALID_HANDLE_VALUE)
	{
		return false;
	}
	CloseHandle(hFile);

	//加载libmysql.dll
	acl::db_handle::set_loadpath(sCurrentPath.c_str());

	return true;
}

int CMysql_acl::mysql_Updata(acl::db_handle& h_db, const char* sql)
{
	int nRet = -1;
	h_db.free_result();
	if ( !h_db.sql_update(sql) )
	{
		return nRet;
	}

	nRet = (int)h_db.affect_count();
	return nRet;
}

int CMysql_acl::mysql_Select(acl::db_handle& h_db, const char* sql)
{
	int nRet = -1;
	h_db.free_result();
	if ( !h_db.sql_select(sql) )
	{
		return nRet;
	}

	m_nCurRow = 0;
	m_nRowCount = h_db.length();
	nRet = (int)m_nRowCount;
	return nRet;
}

bool CMysql_acl::mysql_getNextRow_private(acl::db_handle& h_db)
{
	m_nCurRow++;
	if (m_nCurRow > m_nRowCount)
	{
		h_db.free_result();
		return false;
	}
	return true;
}

int CMysql_acl::mysql_getRowIntValue_private(acl::db_handle& h_db, const char* colName)
{
	int nRet;
	int nRowNo = m_nCurRow - 1;
	if (nRowNo < 0)
	{
		return 0;
	}
	const acl::db_row* row = h_db[nRowNo];
	nRet = (*row).field_int(colName);
	return nRet;
}

double CMysql_acl::mysql_getRowDoubleValue_private(acl::db_handle& h_db, const char* colName)
{
	double dRet;
	int nRowNo = m_nCurRow - 1;
	if (nRowNo < 0)
	{
		return (double)0;
	}
	const acl::db_row* row = h_db[nRowNo];
	dRet = (*row).field_double(colName);
	return dRet;
}

const char* CMysql_acl::mysql_getRowStringValue_private(acl::db_handle& h_db, const char* colName)
{
	int nRowNo = m_nCurRow - 1;
	if (nRowNo < 0)
	{
		return "";
	}
	const acl::db_row* row = h_db[nRowNo];
	const char* ptr = (*row).field_string(colName);
	if (NULL == ptr)
	{
		return "";
	}
	return ptr;
}
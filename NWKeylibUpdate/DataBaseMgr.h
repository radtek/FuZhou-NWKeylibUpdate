#pragma once
#define OTL_ORA9I //连接oracle时
//#define OTL_ODBC // Compile OTL 4/ODBC. Uncomment this when used with MS SQL 7.0/ 2000

#define EXIT_FAILURE_NUM 1
#define RECONNECT_NUM 2

#include "oci/otlv4.h"
#include <string>
using namespace std;

enum Driver { Oracle = 1, SQLServer };
#pragma comment(lib, "oci.lib")


class CDataBaseMgr
{
public:
	CDataBaseMgr(void);
	~CDataBaseMgr(void);
public:

	//设置连接信息
	void SetConnectString(int nDriver, string sServer, string sDatabase, string sUid, string sPwd);

	//连接数据库
	bool ConnectDB();

	//和数据库断开连接
	bool DisconnectDB();

	//重连数据库
	bool ReConnectDB();

	//执行增删改查操作
	bool ExecuteSQL(const char* sql,otl_stream &otlCur, bool bCommit = false);


private:

	string m_sConnection;
	int  m_nDirver;

	int m_nReconnect;   //重连数据库次数

	CRITICAL_SECTION m_clock;
	otl_connect m_db;

    string m_sServer;
    string m_sDateBase;

    time_t m_tConnectDB;
    bool m_bConnectDB;  //当前连接数据库状态, true: 己连接, false: 未连接
public:
    string m_sSIPChannelTable;
    string m_sSIPDVRTable;
};

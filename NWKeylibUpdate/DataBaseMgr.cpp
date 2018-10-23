#include "StdAfx.h"
#include "DataBaseMgr.h"
#include "LogRecorder.h"

extern CLogRecorder g_LogRecorder;
CDataBaseMgr::CDataBaseMgr(void)
{
	m_nReconnect = 0;
	m_nDirver = 0;
	m_sConnection = "";
    m_sSIPChannelTable = "xy_base.sipchannel";
    m_sSIPDVRTable = "xy_base.sipdvr";
    m_sServer = "";
    m_sDateBase = "";
    m_bConnectDB = false;
	InitializeCriticalSection(&m_clock);
}

CDataBaseMgr::~CDataBaseMgr(void)
{
	m_nReconnect = 0;

	DeleteCriticalSection(&m_clock);
}

void CDataBaseMgr::SetConnectString(int nDriver, string sServer, string sDatabase, string sUid, string sPwd)
{
	EnterCriticalSection(&m_clock);
	m_nDirver = nDriver;
    m_sServer = sServer;
    m_sDateBase = sDatabase;
	char sConnect[1024] = {0};
	if (m_nDirver == SQLServer)
	{
		sprintf_s(sConnect, 1024, "driver=SQL Server;Server=%s;Database=%s;Uid=%s;Pwd=%s;", sServer.c_str(), sDatabase.c_str(), sUid.c_str(), sPwd.c_str());
		m_sConnection = sConnect;
	}
	else if (m_nDirver == Oracle)
	{
		sprintf_s(sConnect, 1024, "%s/%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=%s)))",
			sUid.c_str(), sPwd.c_str(), sServer.c_str(), sDatabase.c_str());
		m_sConnection = sConnect;
	}
    g_LogRecorder.WriteInfoLogEx(__FUNCTION__, "连接数据库参数[%s].", sConnect);
	LeaveCriticalSection(&m_clock);

    return;
}

//连接数据库
bool CDataBaseMgr::ConnectDB()
{
	EnterCriticalSection(&m_clock);
	try
	{
        _putenv(const_cast<char*>("NLS_LANG=SIMPLIFIED CHINESE_CHINA.ZHS16GBK"));
        otl_connect::otl_initialize(1); 
        m_db.rlogon(m_sConnection.c_str(),0); 
        m_db.auto_commit_off();
        m_bConnectDB = true;
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "连接数据库[%s:%s]成功.", m_sServer.c_str(), m_sDateBase.c_str());
    }
	catch(otl_exception &p)
	{
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "***Warning: 连接数据库失败!");
		g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--错误代码：%d", p.code);
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--错误信息：%s", p.msg);
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--错误语句：%s", m_sConnection.c_str());
        m_bConnectDB = false;
	}

    LeaveCriticalSection(&m_clock);
	return m_bConnectDB;
}


//重连数据库
bool CDataBaseMgr::ReConnectDB()
{
    time_t tCurrent = time(&tCurrent);
    if(tCurrent - m_tConnectDB < 60)
    {
        g_LogRecorder.WriteInfoLogEx(__FUNCTION__, "重连时间小于60秒, 直接返回.");
        return m_bConnectDB;
    }	

    if (m_bConnectDB && 1 == m_db.connected)
    { 
        m_db.logoff();
    } 
    m_bConnectDB = false;

    do 
    {
        try 
        {
            m_nReconnect ++;
            m_db.rlogon(m_sConnection.c_str(), 0);
            m_bConnectDB = true;
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "重新连接数据库[%s:%s]成功.", m_sServer.c_str(), m_sDateBase.c_str());
            break;
        } 
        catch (otl_exception &p) 
        { 
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "***Warning: 重新连接数据库失败!");
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--错误代码：%d", p.code);
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--错误信息：%s", p.msg);
            m_bConnectDB = false;
            Sleep(3 * 1000); 
        }
    } while (m_nReconnect < RECONNECT_NUM);
    
    m_nReconnect = 0;
    time(&m_tConnectDB);
	return m_bConnectDB;
}

//和数据库断开连接
bool CDataBaseMgr::DisconnectDB()
{
	EnterCriticalSection(&m_clock);
	try
	{
		m_db.commit();
		m_db.logoff();
        m_bConnectDB = false;
	}
	catch(otl_exception &p)
	{
		LeaveCriticalSection(&m_clock);
		return false;
	}
	LeaveCriticalSection(&m_clock);
	return true;
}


//执行SQL语句
bool CDataBaseMgr::ExecuteSQL(const char * sql,otl_stream &otlCur, bool bCommit)
{
    bool bRet = false;
	if(sql == NULL || strlen(sql) <= 5 )
	{
		return bRet;	
	}

    if(!m_bConnectDB)
    {
        if(!ReConnectDB())
        {
            return bRet;
        }
    }

    EnterCriticalSection(&m_clock);
    try
	{
		otlCur.open(1,sql,m_db);
		otlCur.set_commit(0);		
        if(bCommit)
        {
            m_db.commit();
        }
        bRet = true;
	}
	catch(otl_exception &p)
	{ 
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "^^^^Error: 执行SQL语句失败! 错误信息：%s.", p.msg);
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "SQL: %s", sql);
		otlCur.close();

		if (ReConnectDB())
		{
			g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "重连数据库成功.");
            try
            {
                otlCur.open(1,sql,m_db);
                otlCur.set_commit(0);	
                if(bCommit)
                {
                    m_db.commit();
                }
                bRet = true;
            }
            catch(otl_exception &p)
            { 
                g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "^^^^Error: 执行SQL语句失败! 错误信息：%s.", p.msg);
                g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "SQL: %s", sql);
            }
		}
		else
		{
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "****Error: 重连数据库失败.");
		}
	}

	LeaveCriticalSection(&m_clock);
	return bRet;
}

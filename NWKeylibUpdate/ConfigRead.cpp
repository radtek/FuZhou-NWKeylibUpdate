#include "StdAfx.h"
#include "ConfigRead.h"
#include "XSEncrypt/XSEncryptSDK.h"


CConfigRead::CConfigRead(void)
{
    
}

CConfigRead::~CConfigRead(void)
{
}

string CConfigRead::GetCurrentPath()
{
    DWORD nBufferLenth = MAX_PATH;
    char szBuffer[MAX_PATH] = { 0 };
    DWORD dwRet = GetModuleFileNameA(NULL, szBuffer, nBufferLenth);
    char *sPath = strrchr(szBuffer, '\\');
    memset(sPath, 0, strlen(sPath));
    m_sCurrentPath = szBuffer;
    return m_sCurrentPath;
}


bool CConfigRead::ReadConfig()
{
    m_sConfigFile = m_sCurrentPath + "/config/Config.xml";
#ifdef _DEBUG
    m_sConfigFile = "./config/Config.xml";
#endif
    try
    {
        ptree pt;
        read_xml(m_sConfigFile, pt, boost::property_tree::xml_parser::trim_whitespace);

        char pTemp[2048] = { 0 };

        m_sDBIP = pt.get<string>("Config.MySQL.IP");
        if (m_sDBIP.size() < 36)      //Œ¥º”√‹◊¥Ã¨
        {
            XSEncrypt(m_sDBIP.c_str(), pTemp, NULL);
            pt.put<string>("Config.MySQL.IP", pTemp);
        }
        else                        //º”√‹◊¥Ã¨, ‘ÚΩ‚√‹
        {
            XSDecrypt(m_sDBIP.c_str(), pTemp, NULL);
            m_sDBIP = pTemp;
        }

        string sDBPort = pt.get<string>("Config.MySQL.Port");
        if (sDBPort.size() < 36)      //Œ¥º”√‹◊¥Ã¨
        {
            XSEncrypt(sDBPort.c_str(), pTemp, NULL);
            m_nDBPort = atoi(sDBPort.c_str());
            pt.put<string>("Config.MySQL.Port", pTemp);
        }
        else                        //º”√‹◊¥Ã¨
        {
            XSDecrypt(sDBPort.c_str(), pTemp, NULL);
            m_nDBPort = atoi(pTemp);
        }

        m_sDBName = pt.get<string>("Config.MySQL.Name");
        if (m_sDBName.size() < 36)      //Œ¥º”√‹◊¥Ã¨
        {
            XSEncrypt(m_sDBName.c_str(), pTemp, NULL);
            pt.put<string>("Config.MySQL.Name", pTemp);
        }
        else                        //º”√‹◊¥Ã¨
        {
            XSDecrypt(m_sDBName.c_str(), pTemp, NULL);
            m_sDBName = pTemp;
        }

        m_sDBUid = pt.get<string>("Config.MySQL.UserID");
        if (m_sDBUid.size() < 36)
        {
            XSEncrypt(m_sDBUid.c_str(), pTemp, NULL);
            pt.put<string>("Config.MySQL.UserID", pTemp);
        }
        else
        {
            XSDecrypt(m_sDBUid.c_str(), pTemp, NULL);
            m_sDBUid = pTemp;
        }

        m_sDBPwd = pt.get<string>("Config.MySQL.Password");
        if (m_sDBPwd.size() < 36)
        {
            XSEncrypt(m_sDBPwd.c_str(), pTemp, NULL);
            pt.put<string>("Config.MySQL.Password", pTemp);
        }
        else
        {
            XSDecrypt(m_sDBPwd.c_str(), pTemp, NULL);
            m_sDBPwd = pTemp;
        }

        boost::property_tree::xml_writer_settings<char> settings('\t', 1);
        boost::property_tree::write_xml(m_sConfigFile, pt, std::locale(), settings);     
        
        m_sOracleIP = pt.get<string>("Config.Oracle.OracleIP");
        m_sOracleName = pt.get<string>("Config.Oracle.OracleName");
        m_sOracleUser = pt.get<string>("Config.Oracle.OracleUser");
        m_sOraclePassword = pt.get<string>("Config.Oracle.OraclePassword");

        m_sSavePath = pt.get<string>("Config.SavePath");
        if ("" == m_sSavePath)
        {
            m_sSavePath = "D:/XS-Face";
        }
        m_sBatchStoreServerIP = pt.get<string>("Config.BatchStoreServerIP");
        m_nBatchStoreServerPort = pt.get<int>("Config.BatchStoreServerPort");
        m_nWaitTime = pt.get<int>("Config.WaitTime");
    }
    catch (boost::property_tree::ptree_error e)
    {
        printf("****Error: ∂¡»°≈‰÷√Œƒº˛≥ˆ¥Ì, %s!", e.what());
        return false;
    }

    if (m_sDBIP == "")
    {
        return false;
    }

    return true;
}

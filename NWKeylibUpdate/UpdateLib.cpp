#include "StdAfx.h"
#include "UpdateLib.h"


CLogRecorder g_LogRecorder;
CUpdateLib::CUpdateLib()
{
    m_pDBInfo = NULL;
    m_hStopEvent = CreateEvent(NULL, true, false, NULL);
}

CUpdateLib::~CUpdateLib()
{
    if (NULL != m_pDBInfo)
    {
        delete m_pDBInfo;
        m_pDBInfo = NULL;
    }
    SetEvent(m_hStopEvent);
    CloseHandle(m_hStopEvent);
}
bool CUpdateLib::StartUpdate()
{
    m_pConfigRead = new CConfigRead;
    string sPath = m_pConfigRead->GetCurrentPath();
    string sConfigPath = sPath + "/Config/NWKeylibUpdate_config.properties";
#ifdef _DEBUG
    sConfigPath = "./Config/NWKeylibUpdate_config.properties";
#endif
    g_LogRecorder.InitLogger(sConfigPath.c_str(), "NWKeylibUpdateLogger", "NWKeylibUpdate");

    //��ȡ�����ļ�
    if (!m_pConfigRead->ReadConfig())
    {
        g_LogRecorder.WriteErrorLogEx(__FUNCTION__, "****Error: ��ȡ�����ļ���������!");
        return false;
    }
    else
    {
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "��ȡ�����ļ��ɹ�.");
    }

    m_pDBInfo = new UPDATEDBINFO;
    strcpy(m_pDBInfo->pOracleDBIP, m_pConfigRead->m_sOracleIP.c_str());
    strcpy(m_pDBInfo->pOracleDBName, m_pConfigRead->m_sOracleName.c_str());
    strcpy(m_pDBInfo->pOracleDBUser, m_pConfigRead->m_sOracleUser.c_str());
    strcpy(m_pDBInfo->pOracleDBPassword, m_pConfigRead->m_sOraclePassword.c_str());
    strcpy(m_pDBInfo->pMysqlDBIP, m_pConfigRead->m_sDBIP.c_str());
    strcpy(m_pDBInfo->pMysqlDBName, m_pConfigRead->m_sDBName.c_str());
    strcpy(m_pDBInfo->pMysqlDBUser, m_pConfigRead->m_sDBUid.c_str());
    strcpy(m_pDBInfo->pMysqlDBPassword, m_pConfigRead->m_sDBPwd.c_str());
    m_pDBInfo->nMysqlDBPort = m_pConfigRead->m_nDBPort;
    strcpy(m_pDBInfo->pSavePath, m_pConfigRead->m_sSavePath.c_str());
    strcpy(m_pDBInfo->pBatchStoreServerIP, m_pConfigRead->m_sBatchStoreServerIP.c_str());
    m_pDBInfo->nBatchStoreServerPort = m_pConfigRead->m_nBatchStoreServerPort;

    do 
    {
        //����Oracle
        m_DBMgr.SetConnectString(Oracle, m_pDBInfo->pOracleDBIP, m_pDBInfo->pOracleDBName, m_pDBInfo->pOracleDBUser, m_pDBInfo->pOracleDBPassword);
        if (!m_DBMgr.ConnectDB())
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Error : ����oracle���ݿ�[%s]ʧ��!", m_pDBInfo->pOracleDBIP);
            return false;
        }
        else
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Success : ����oracle���ݿ�[%s]�ɹ�!", m_pDBInfo->pOracleDBIP);
        }

        //����MySQL���ݿ�
        if (!m_mysqltool.mysql_connectDB(m_pDBInfo->pMysqlDBIP, m_pDBInfo->nMysqlDBPort, m_pDBInfo->pMysqlDBName,
            m_pDBInfo->pMysqlDBUser, m_pDBInfo->pMysqlDBPassword, "gb2312"))
        {
            g_LogRecorder.WriteErrorLogEx(__FUNCTION__, "****Error: ����Mysql���ݿ�ʧ��[%s:%d:%s]!",
                m_pDBInfo->pMysqlDBIP, m_pDBInfo->nMysqlDBPort, m_pDBInfo->pMysqlDBName);
            return false;
        }
        else
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "����Mysql���ݿ�ɹ�[%s:%d:%s].",
                m_pDBInfo->pMysqlDBIP, m_pDBInfo->nMysqlDBPort, m_pDBInfo->pMysqlDBName);
        }


        //ͬ��Oracle����������Ϣ��Mysql
        if (!SyncKeyLibInfo())
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Error : ͬ������������Ϣʧ��!");
            return false;
        }
        //��������ͼƬ�����ر���
        if (!DownloadAddImage())
        {
            g_LogRecorder.WriteDebugLog(__FUNCTION__, "Error: ��������ͼƬ���浽����ʧ��!");
            return false;
        }
        //������ͼƬ��Ϣ���͵��������ط���
        if (!AddBatchInfo())
        {
            return false;
        }
        //ɾ�����಼����Ϣ���������ط���
        if (!DelBatchInfo())
        {
            return false;
        }
        //����StoreCount�����ͼƬ����
        if (!UpdateStoreCount())
        {
            return false;
        }
        //����������Ϣ, Ϊ��һ�θ���׼��
        if (!ClearInfo())
        {
            return false;
        }

    } while (WAIT_TIMEOUT == WaitForSingleObject(m_hStopEvent, 1000 *m_pConfigRead->m_nWaitTime));
    

    //���º������Ⲽ��ͼƬ���浽����
    /*if (!UpdateKeyLib())
    {
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Error : ���º������Ⲽ����Ϣʧ��!");
        return false;
    }*/
    return true;
}
bool CUpdateLib::StopUpdate()
{
    ClearInfo();
    SetEvent(m_hStopEvent);
    return true;
}
//����������Ϣ, Ϊ��һ�θ���׼��
bool CUpdateLib::ClearInfo()
{
    m_nLayoutSuccess = 0;
    m_nLayoutFailed = 0;
    m_mapLayoutBH.clear();
    m_setga_ztryxx.clear();
    MAPZDRYINFO::iterator it = m_mapZDRYInfo.begin();
    while(it != m_mapZDRYInfo.end())
    {
        delete it->second;
        it = m_mapZDRYInfo.erase(it);
    }
    m_DBMgr.DisconnectDB();
    m_mysqltool.mysql_disconnectDB();
    return true;
}
//ͬ��Oracle����������Ϣ��Mysql
bool CUpdateLib::SyncKeyLibInfo()
{
    g_LogRecorder.WriteDebugLog(__FUNCTION__, "��ʼͬ������������Ϣ...");
    int nRet = 0;
    int nNumber = 0;
    char pSql[1024 * 8] = { 0 };
    char pZDRYBH[64] = { 0 };
    char pFaceUUID[64] = { 0 };

    //��ȡga_ztryxx�������Ϣ�����ڲ���ʱ�ж��Ƿ���Ҫ����
    sprintf_s(pSql, sizeof(pSql),
        "Select ztrybh from %s", MYSQLUPDATETABLE);
    nRet = m_mysqltool.mysql_exec(_MYSQL_SELECT, pSql);
    if (nRet > 0)
    {
        while (m_mysqltool.mysql_getNextRow())
        {
            strcpy_s(pZDRYBH, sizeof(pZDRYBH), m_mysqltool.mysql_getRowStringValue("ztrybh"));
            m_setga_ztryxx.insert(pZDRYBH);
        }
    }

    //��ȡmysql storefaceinfo�������ص���Ա��Ϣ, ���ں���ĸ��»����;
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "��ȡMysql���ݿ����������ԱSFZ��Ϣ...");
    sprintf_s(pSql, sizeof(pSql),
        "Select zdrybh, faceuuid from %s where LayoutLibId = %d and zdrybh is not null", MYSQLSTOREFACEINFO, ZTRYSTORELIBID);
    nRet = m_mysqltool.mysql_exec(_MYSQL_SELECT, pSql);
    if (nRet > 0)
    {
        while (m_mysqltool.mysql_getNextRow())
        {
            strcpy_s(pZDRYBH, sizeof(pZDRYBH), m_mysqltool.mysql_getRowStringValue("zdrybh"));
            strcpy_s(pFaceUUID, sizeof(pFaceUUID), m_mysqltool.mysql_getRowStringValue("faceuuid"));
            //�����������ڲ��ص���Ա�����Ϣ, �����ж��Ƿ���Ҫɾ�����������
            m_mapLayoutBH.insert(make_pair(pZDRYBH, pFaceUUID));
        }
    }
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "��ȡMysql���ݿ����ӿ���Ա�����Ϣ����, ��Ա����[%d].", m_mapLayoutBH.size());

    //��������ͼƬ�ļ��ӵ�ַ
    SYSTEMTIME sysTime;
    GetLocalTime(&sysTime);
    char pTime[20] = { 0 };
    sprintf_s(pTime, sizeof(pTime), "%04d%02d%02d%02d%02d%02d/", 
        sysTime.wYear, sysTime.wMonth, sysTime.wDay, sysTime.wHour, sysTime.wMinute, sysTime.wSecond);
    string sZTPath = m_pDBInfo->pSavePath;
    string sZTYXPath = m_pDBInfo->pSavePath + string("0/"); //������Ч
    string sZTYXCurTimePath = m_pDBInfo->pSavePath + string("0/") + string(pTime);  //������Ч���ո���ʱ��
    CreateDirectory(sZTPath.c_str(), NULL);
    CreateDirectory(sZTYXPath.c_str(), NULL);
    CreateDirectory(sZTYXCurTimePath.c_str(), NULL);
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "�����ļ���[%s]�ɹ�.", sZTYXCurTimePath.c_str());

    //��ȡoracle������������Ա��Ϣ(NVL�����������Щ�ֶ�Ϊ��ʱ, ��ȡʱ�����, Ϊ�յ��ֶ�ͨ��NVLָ��һ��Ĭ��ֵ)
    sprintf_s(pSql, sizeof(pSql),
        "select ZTRYBH,NVL(XM, 'default'),NVL(BMCH, 'default'),NVL(XBDM, 'default'), "
        "NVL(MZDM, 'default'), NVL(CSRQ, '0-0-0'),NVL(HJDZ_XZQHDM, 'default'),NVL(HJDZ_DZMC, 'default'), "
        "NVL(XZZ_XZQHDM, 'default'), NVL(XZZ_DZMC, 'default'),SFZH,NVL(CYZJ, 'default'), "
        "NVL(SGXX, 'default'),NVL(SGSX, 'default'), NVL(ASJBH, 'default'),NVL(TPSJ, '0-0-0'), "
        "NVL(TPFX_JYQK, 'default'),NVL(TJJBDM, 'default'), NVL(ZTJJ, 'default'), "
        "NVL(ZTRYLXDM, 'default'), NVL(LADW_GAJGJGDM, 'default'),NVL(ZBR_XM, 'default'),NVL(ZBR_LXDH, 'default'), "
        "NVL(ZBRE_XM, 'default'), to_char(JL_GXSJ, 'yyyy-mm-dd HH24:mi:ss'),NVL(JL_ZJBM, 'default'),to_char(JL_RKSJ, 'yyyy-mm-dd HH24:mi:ss'), "
        "NVL(JL_FILENAME, 'default'), NVL(YWZT, 'default'),NVL(HJDZ_XZQHDM_CN, 'default'), NVL(XBDM_CN, 'default'), "
        "NVL(LADW_GAJGJGDM_CN, 'default'),NVL(TPFX_JYQK_CN, 'default'), NVL(XZZ_XZQHDM_CN, 'default'), "
        "NVL(MZDM_CN, 'default'),NVL(ZTRYLXDM_CN, 'default'),NVL(SFZ18, 'default')  "
        "from %s where BH like '%%_1' and SFZH is not NULL and ZP is not NULL and YWZT = '0'", NWORACLEZTYXTABLE);
    otl_stream otlSelect;
    if (!m_DBMgr.ExecuteSQL(pSql, otlSelect))
    {
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ִ��SQL���ʧ��!");
        return false;
    }
    else
    {
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ִ��SQL���, ��ȡOracle����������Ա��Ϣ��...!");
    }
    char ZTRYBH[256] = {0}; char XM[256] = {0}; char BMCH[256] = {0}; char XBDM[256] = {0}; char MZDM[256] = {0};
    char CSRQ[256] = {0}; char HJDZ_XZQHDM[256] = {0}; char HJDZ_DZMC[256] = {0}; 
    char XZZ_XZQHDM[256] = {0}; char XZZ_DZMC[256] = {0}; char SFZH[256] = {0}; char CYZJ[256] = {0};
    char SGXX[256] = {0};  char SGSX[256] = {0}; char ASJBH[256] = {0}; char TPSJ[256] = {0}; 
    char TPFX_JYQK[256] = {0}; char TJJBDM[256] = {0}; char ZTJJ[256] = {0}; char ZTRYLXDM[256] = {0}; 
    char LADW_GAJGJGDM[256] = {0}; char ZBR_XM[256] = {0}; char ZBR_LXDH[256] = {0}; char ZBRE_XM[256] = {0}; 
    char JL_GXSJ[256] = {0}; char JL_ZJBM[256] = {0}; char JL_RKSJ[256] = {0}; 
    char JL_FILENAME[256] = {0}; char YWZT[256] = {0}; char HJDZ_XZQHDM_CN[256] = {0}; char XBDM_CN[256] = {0}; 
    char LADW_GAJGJGDM_CN[256] = {0}; char TPFX_JYQK_CN[256] = {0}; char XZZ_XZQHDM_CN[256] = {0}; char MZDM_CN[256] = {0}; 
    char ZTRYLXDM_CN[256] = {0}; char SFZ18[256] = {0};

    int nAdd = 0;
    while (!otlSelect.eof())
    {
        otlSelect >> ZTRYBH >> XM >> BMCH >> XBDM >> MZDM >> CSRQ >> HJDZ_XZQHDM >> HJDZ_DZMC >> XZZ_XZQHDM >>
            XZZ_DZMC >> SFZH >> CYZJ >> SGXX >> SGSX >> ASJBH >> TPSJ >> TPFX_JYQK >> TJJBDM >> ZTJJ >> ZTRYLXDM >>
            LADW_GAJGJGDM >> ZBR_XM >> ZBR_LXDH >> ZBRE_XM >> JL_GXSJ >> JL_ZJBM >> JL_RKSJ >> JL_FILENAME >> YWZT >>
            HJDZ_XZQHDM_CN >> XBDM_CN >> LADW_GAJGJGDM_CN >> TPFX_JYQK_CN >> XZZ_XZQHDM_CN >> MZDM_CN >> ZTRYLXDM_CN >> SFZ18;

        map<string, string>::iterator it = m_mapLayoutBH.find(ZTRYBH);
        set<string>::iterator itJudeg = m_setga_ztryxx.begin();
        //����֤��ż����������, δ���������Mysqlͬ����
        if (it != m_mapLayoutBH.end())
        {
            /*sprintf_s(pSql, sizeof(pSql),
            "update %s set sfzh = '%s' where ztrybh = '%s'", MYSQLUPDATETABLE, SFZH, ZTRYBH);*/
            m_mapLayoutBH.erase(it);
        }
        else
        {
            nAdd++;

            LPZDRYINFO pZDRYInfo = new ZDRYINFO;
            strcpy(pZDRYInfo->pName, XM);
            strcpy(pZDRYInfo->pSex, XBDM);
            strcpy(pZDRYInfo->pSFZH, SFZH);
            strcpy(pZDRYInfo->pAddress, XZZ_DZMC);
            string sTempPath = sZTYXCurTimePath + string(ZTRYBH) + string(".jpg");
            strcpy(pZDRYInfo->pImagePath, sTempPath.c_str());
            m_mapZDRYInfo.insert(make_pair(ZTRYBH, pZDRYInfo));

            itJudeg = m_setga_ztryxx.find(ZTRYBH);  //�ж���ga_ztryxx���Ƿ񼺴�����������, �����������
            if(itJudeg == m_setga_ztryxx.end())
            {
                sprintf_s(pSql, sizeof(pSql),
                    "INSERT INTO %s(ZTRYBH,XM,BMCH,XBDM,MZDM,CSRQ,HJDZ_XZQHDM,HJDZ_DZMC,XZZ_XZQHDM, "
                    "XZZ_DZMC,SFZH,CYZJ,SGXX,SGSX,ASJBH,TPSJ,TPFX_JYQK,TJJBDM,ZTJJ,ZTRYLXDM, "
                    "LADW_GAJGJGDM,ZBR_XM,ZBR_LXDH,ZBRE_XM,JL_GXSJ,JL_ZJBM,JL_RKSJ,JL_FILENAME,YWZT,HJDZ_XZQHDM_CN, "
                    "XBDM_CN,LADW_GAJGJGDM_CN,TPFX_JYQK_CN,XZZ_XZQHDM_CN,MZDM_CN,ZTRYLXDM_CN,SFZ18) "
                    "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
                    "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
                    "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                    MYSQLUPDATETABLE,
                    ZTRYBH, XM, BMCH, XBDM, MZDM, CSRQ, HJDZ_XZQHDM, HJDZ_DZMC, XZZ_XZQHDM,
                    XZZ_DZMC, SFZH, CYZJ, SGXX, SGSX, ASJBH, TPSJ, TPFX_JYQK, TJJBDM, ZTJJ, ZTRYLXDM,
                    LADW_GAJGJGDM, ZBR_XM, ZBR_LXDH, ZBRE_XM, JL_GXSJ, JL_ZJBM, JL_RKSJ, JL_FILENAME, YWZT,
                    HJDZ_XZQHDM_CN, XBDM_CN, LADW_GAJGJGDM_CN, TPFX_JYQK_CN, XZZ_XZQHDM_CN, MZDM_CN, ZTRYLXDM_CN, SFZ18);


                nRet = m_mysqltool.mysql_exec(_MYSQL_INSERT, pSql);
                if (nRet < 0)
                {
                    g_LogRecorder.WriteWarnLogEx(__FUNCTION__, "***Warning: Oracle���ݿ�ͬ���ص���Ա��Ϣ��Mysql���ݱ�ʧ��.");
                }
            }
        }

        nNumber++;
        if (nNumber % 30000 == 0)
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "��ͬ����Ա��Ϣ: %d", nNumber);
        }
    }
    otlSelect.close();

    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ͬ������������Ա��Ϣ������, ͬ��[%d], Add[%d].", nNumber, nAdd);
    return true;
}
//��������ͬ������ͼƬ�����ر���
bool CUpdateLib::DownloadAddImage()
{
    g_LogRecorder.WriteDebugLog(__FUNCTION__, "��ʼ������������ͼƬ...");
   
    DWORD dwWrite = 0;
    int nLen = 0;
    otl_long_string sZP;
    otl_stream otlSelect;
    string sTempPath = "";

    char pSql[1024 * 8] = { 0 };
    int nAdd = 0;
    MAPZDRYINFO::iterator it = m_mapZDRYInfo.begin();
    for (; it != m_mapZDRYInfo.end(); it++)
    {
        sprintf_s(pSql, sizeof(pSql),
            "select ZP from %s where ZTRYBH = '%s' and BH like '%%_1'", NWORACLEZTYXTABLE, it->first.c_str());
        if (!m_DBMgr.ExecuteSQL(pSql, otlSelect))
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ִ��SQL���ʧ��!");
            return false;
        }

        while (!otlSelect.eof())
        {
            otlSelect >> sZP;
            nLen = sZP.length;
            if (0 == nLen)
            {
                break;
            }

            HANDLE hFileHandle = CreateFile(it->second->pImagePath,
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                NULL,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                NULL);

            WriteFile(hFileHandle, sZP.v, nLen, &dwWrite, NULL);
            CloseHandle(hFileHandle);

            nAdd++;
            if(nAdd % 2000 == 0)
            {
                g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Aleady Download Image: %d.", nAdd);
            }
        }
        otlSelect.close();
    }
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Download Add Image Finished, Count[%d].", nAdd);
    return true;
}
//����������Ϣ����������
bool CUpdateLib::AddBatchInfo()
{
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "���Ӳ�������ͼƬ��Ϣ���������ط���, ����[%d].", m_mapZDRYInfo.size());
    char pHttpURL[128] = { 0 };
    sprintf_s(pHttpURL, sizeof(pHttpURL), "http://%s:%d/Store/addpicture", m_pDBInfo->pBatchStoreServerIP, m_pDBInfo->nBatchStoreServerPort);
    CHttpSyncClientPtr pClient(NULL);

    rapidjson::Document document;
    document.SetObject();
    rapidjson::Document::AllocatorType&allocator = document.GetAllocator();

    string sRespon = "";
    char * pImageInfo = new char[MAXIMAGESIZE];
    char * pImageBase64[10];
    for (int i = 0; i < 10; i++)
    {
        pImageBase64[i] = new char[MAXIMAGESIZE];
    }
    DWORD * pRealRead = new DWORD;
    int nFileSize = 0;
    int nLayout = 0;
    m_nLayoutFailed = 0;
    m_nLayoutSuccess = 0;
    MAPZDRYINFO::iterator it = m_mapZDRYInfo.begin();

    while (it != m_mapZDRYInfo.end())
    {
        rapidjson::Value array(rapidjson::kArrayType);
        int i = 0;
        while (it != m_mapZDRYInfo.end())
        {
            if (i < 10)
            {
                //��ȡͼƬ����������
                HANDLE hFileHandle = CreateFile(it->second->pImagePath,
                    GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    NULL);
                if (INVALID_HANDLE_VALUE != hFileHandle)
                {
                    nFileSize = GetFileSize(hFileHandle, NULL);
                    if (nFileSize < MAXIMAGESIZE)
                    {
                        ReadFile(hFileHandle, pImageInfo, nFileSize, pRealRead, NULL);

                        //������������Base64����, ���㴫��
                        string sFeature = ZBase64::Encode((BYTE*)pImageInfo, nFileSize);
                        strcpy(pImageBase64[i], sFeature.c_str());
                        pImageBase64[i][sFeature.size()] = '\0';
                        nLayout ++;

                        //����Json������
                        rapidjson::Value object(rapidjson::kObjectType);
                        object.AddMember("Face", rapidjson::StringRef(pImageBase64[i]), allocator);
                        object.AddMember("Name", rapidjson::StringRef(it->first.c_str()), allocator);
                        array.PushBack(object, allocator);

                        //g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Add Image[%s].", it->second->pImagePath);
                    }
                    else
                    {
                        g_LogRecorder.WriteWarnLogEx(__FUNCTION__, "***Warning: ͼƬ���ݹ���[%s].", it->second->pImagePath);
                    }
                    CloseHandle(hFileHandle);
                }
                
                i++;
                it++;
                if (i < 10 && it != m_mapZDRYInfo.end())
                {
                    continue;
                }
            }

            document.AddMember(JSONSTORELIBID, ZTRYSTORELIBID, allocator);
            document.AddMember(JSONLIBTYPE, 3, allocator);
            document.AddMember(JSONSTOREPHOTO, array, allocator);

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            document.Accept(writer);
            string sDelInfo = string(buffer.GetString());

            //���������ط�������Ϣ
            pClient->OpenUrl("POST", pHttpURL, NULL, 0, (const BYTE*)sDelInfo.c_str(), sDelInfo.size());
            //g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "���������ط��������Ӳ���������Ϣ...");

            //��ȡ��Ӧ��Ϣ
            LPCBYTE pRespon = NULL;
            int nRepSize = 0;
            pClient->GetResponseBody(&pRespon, &nRepSize);
            sRespon.assign((char*)pRespon, nRepSize);
            //g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "�������ط����Ӧ���Ӳ���������Ϣ...");
            //������Ӧ��Ϣ
            InsertAddInfoToDB((char*)sRespon.c_str());

            if(nLayout % 1000 == 0)
            {
                g_LogRecorder.WriteDebugLogEx(__FUNCTION__, 
                    "Total Count[%d], Handle[%d], Success[%d], Failed[%d].", 
                    m_mapZDRYInfo.size(), nLayout, m_nLayoutSuccess, m_nLayoutFailed);
            }
            i = 0;
            document.RemoveAllMembers();
            break;
        }
    }

    delete[]pImageInfo;
    for (int i = 0; i < 10; i++)
    {
        delete[]pImageBase64[i];
    }
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, 
        "��������ͼƬ������, Total Count[%d], Handle[%d], Success[%d], Failed[%d].",
        m_mapZDRYInfo.size(), nLayout, m_nLayoutSuccess, m_nLayoutFailed);
    return true;
}
bool CUpdateLib::InsertAddInfoToDB(char * pMsg)
{
    rapidjson::Document document;
    document.Parse(pMsg);
    if (document.HasParseError())
    {
        g_LogRecorder.WriteWarnLogEx(__FUNCTION__, "����Json��ʧ��[%s]", pMsg);
        return false;
    }

    if (document.HasMember("ErrorMessage") && document["ErrorMessage"].IsString())
    {
        string sErrorMsg = document["ErrorMessage"].GetString();
        g_LogRecorder.WriteInfoLogEx(__FUNCTION__, "***Warning: ���������񷵻���������������Ϣ[%s]", sErrorMsg.c_str());
        m_nLayoutFailed += 10;
        return false;
    }
    else
    {
        if (document.HasMember("Photo") && document["Photo"].IsArray() && document["Photo"].Size() > 0)
        {
            char pSQL[8192] = { 0 };
            int nRet = 0;

            //��ȡ���ص�ǰʱ��
            SYSTEMTIME sysTime;
            GetLocalTime(&sysTime);
            char pTime[32] = { 0 };
            sprintf_s(pTime, sizeof(pTime), "%04d-%02d-%02d %02d:%02d:%02d",
                sysTime.wYear, sysTime.wMonth, sysTime.wDay,
                sysTime.wHour, sysTime.wMinute, sysTime.wSecond);

            for (int i = 0; i < document["Photo"].Size(); i++)
            {
                if (document["Photo"][i].HasMember("Name") && document["Photo"][i]["Name"].IsString() &&
                    document["Photo"][i].HasMember("FaceUUID") && document["Photo"][i]["FaceUUID"].IsString() &&
                    document["Photo"][i].HasMember("SavePath") && document["Photo"][i]["SavePath"].IsString() &&
                    document["Photo"][i].HasMember("face_url") && document["Photo"][i]["face_url"].IsString() &&
                    document["Photo"][i].HasMember("errormessage") && document["Photo"][i]["errormessage"].IsString())
                {
                    string sName = document["Photo"][i]["Name"].GetString();
                    string sFaceUUID = document["Photo"][i]["FaceUUID"].GetString();
                    string sSavePath = document["Photo"][i]["SavePath"].GetString();
                    string sFaceURL = document["Photo"][i]["face_url"].GetString();
                    string sErrorMsg = document["Photo"][i]["errormessage"].GetString();
                    if (sFaceUUID != "")
                    {
                        MAPZDRYINFO::iterator it = m_mapZDRYInfo.find(sName);
                        if (it != m_mapZDRYInfo.end())
                        {
                            sprintf_s(pSQL, sizeof(pSQL),
                                "INSERT into %s(faceuuid, imageid, time, localpath, feature, username, sexradio, idcard, "
                                "address, layoutlibid, controlstatus, updatetime, imageip, face_url, zdrybh) "
                                "VALUES ('%s', '', '%s', '%s', '', '%s', '%s', '%s', "
                                "'%s', %d, 0, '%s', '%s', '%s', '%s')",
                                MYSQLSTOREFACEINFO, sFaceUUID.c_str(), pTime, sSavePath.c_str(), it->second->pName, it->second->pSex, it->second->pSFZH,
                                it->second->pAddress, ZTRYSTORELIBID, pTime, m_pDBInfo->pBatchStoreServerIP, sFaceURL.c_str(), it->first.c_str());
                            nRet = m_mysqltool.mysql_exec(_MYSQL_INSERT, pSQL);
                            if (nRet < 0)
                            {
                                g_LogRecorder.WriteWarnLogEx(__FUNCTION__, "***Warning: ���سɹ�������Ϣ���뵽���ݿ�ʧ��.\n%s", pSQL);
                                m_nLayoutFailed ++;
                            }
                            else
                            {
                                m_nLayoutSuccess ++;
                            }
                        }
                    }
                    else
                    {
                        g_LogRecorder.WriteInfoLogEx(__FUNCTION__, "[%s]���ʧ��, ErrorMsg: %s.", sName.c_str(), sErrorMsg.c_str());
                        m_nLayoutFailed ++;
                    }
                }
            }
        }

    }

    return true;
}
//ɾ�����಼����Ϣ���������ط���
bool CUpdateLib::DelBatchInfo()
{
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ɾ�����಼����Ϣ, Count[%d].", m_mapLayoutBH.size());
    char pHttpURL[128] = { 0 };
    sprintf_s(pHttpURL, sizeof(pHttpURL), "http://%s:%d/Store/delpicture", m_pDBInfo->pBatchStoreServerIP, m_pDBInfo->nBatchStoreServerPort);
    CHttpSyncClientPtr pClient(NULL);

    rapidjson::Document document;
    document.SetObject();
    rapidjson::Document::AllocatorType&allocator = document.GetAllocator();

    map<string, string>::iterator it = m_mapLayoutBH.begin();
    while (it != m_mapLayoutBH.end())
    {
        rapidjson::Value array(rapidjson::kArrayType);
        int i = 0;
        while (it != m_mapLayoutBH.end())
        {
            if (i < 10)
            {
                rapidjson::Value object(rapidjson::kObjectType);
                object.AddMember(JSONFACEUUID, rapidjson::StringRef(it->second.c_str()), allocator);
                array.PushBack(object, allocator);
                i++;
                it++;
                if (i < 10 && it != m_mapLayoutBH.end())
                {
                    continue;
                }
            }

            if (i == 10 || it == m_mapLayoutBH.end())
            {
                document.AddMember(JSONSTORELIBID, ZTRYSTORELIBID, allocator);
                document.AddMember(JSONSTOREFACE, array, allocator);

                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                document.Accept(writer);
                string sDelInfo = string(buffer.GetString());

                //���������ط�������Ϣ
                pClient->OpenUrl("POST", pHttpURL, NULL, 0, (const BYTE*)sDelInfo.c_str(), sDelInfo.size());
                //g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "--���������ط�����ɾ����Ϣ...");

                //��ȡ��Ӧ��Ϣ
                LPCBYTE pImageBuf = NULL;
                int nImageLen = 0;
                pClient->GetResponseBody(&pImageBuf, &nImageLen);
                //g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "�������ط����Ӧɾ����Ϣ...");

                i = 0;
                document.RemoveAllMembers();
                break;
            }
        }
    }
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "���������ط�����ɾ����Ϣ����!");

    //�����ݿ�ɾ�����಼������
    char pSql[8096] = { 0 };
    int nRet = 0;
    map<string, string>::iterator itDel = m_mapLayoutBH.begin();
    int nDelete = 0;    //ɾ�������ص���Ա����
    for (; itDel != m_mapLayoutBH.end(); itDel++)
    {
        nDelete++;
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ɾ�����಼����Ϣ, BH[%s].", itDel->first.c_str());

        //ɾ��storefaceinfo������
        sprintf_s(pSql, sizeof(pSql), "delete from %s where zdrybh = '%s'", MYSQLSTOREFACEINFO, itDel->first.c_str());
        nRet = m_mysqltool.mysql_exec(_MYSQL_DELETE, pSql);
        if (nRet < 0)
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Failed: %s", pSql);
            return false;
        }

        //ɾ��ga_zdryxx������
        sprintf_s(pSql, sizeof(pSql), "delete from %s where ztrybh = '%s'", MYSQLUPDATETABLE, itDel->first.c_str());
        nRet = m_mysqltool.mysql_exec(_MYSQL_DELETE, pSql);
        if (nRet < 0)
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Failed: %s", pSql);
            return false;
        }
        //ɾ��layoutresult������
        sprintf_s(pSql, sizeof(pSql), "delete from %s where layoutfaceuuid = '%s'", MYSQLLAYOUTRESULT, itDel->second.c_str());
        nRet = m_mysqltool.mysql_exec(_MYSQL_DELETE, pSql);
        if (nRet < 0)
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "Failed: %s", pSql);
            return false;
        }
    }
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ɾ�����಼����������, ɾ������: %d.", nDelete);
    return true;
}
//���¿�����������storecount��
bool CUpdateLib::UpdateStoreCount()
{
    char pSql[1024 * 8] = { 0 };

    sprintf_s(pSql, sizeof(pSql),
        "update %s set count = "
        "(select count(*) from %s where layoutlibid = %d) "
        "where storelibid = %d", 
        MYSQLSTORECOUNT, MYSQLSTOREFACEINFO, ZTRYSTORELIBID, ZTRYSTORELIBID);
    int nRet = m_mysqltool.mysql_exec(_MYSQL_UPDATA, pSql);
    if (nRet < 0)
    {
        g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "****Failed: %s", pSql);
        return false;
    }
    return true;
}
//���º������Ⲽ��ͼƬ���浽����
bool CUpdateLib::UpdateKeyLib()
{
    //�����ļ���
    string sZTPath = m_pDBInfo->pSavePath;
    string sZTYXPath = m_pDBInfo->pSavePath + string("0/");       //������Ч
    string sZTCXPath = m_pDBInfo->pSavePath + string("1/");       //���ӳ���
    CreateDirectory(sZTPath.c_str(), NULL);
    CreateDirectory(sZTYXPath.c_str(), NULL);
    CreateDirectory(sZTCXPath.c_str(), NULL);

    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "�����ļ���[%s]�ɹ�.", sZTYXPath.c_str());
    g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "�����ļ���[%s]�ɹ�.", sZTCXPath.c_str());


    g_LogRecorder.WriteDebugLog(__FUNCTION__, "��ʼ���º�����������Ϣ...");
    char pSql[1024 * 8] = { 0 };
    char ZTRYBH[64] = { 0 };
    char pZTType[10] = { 0 };
    int nZTType = 0;        //0: ������Ч, 1: ���ӳ���
    DWORD dwWrite = 0;
    int nLen = 0;
    otl_long_string sZP;
    otl_stream otlSelect;

    string sFilePath;
    string sTempPath;
    for (int i = 0; i < 1000 * 1000; i += 1000)
    {
        sprintf_s(pSql, sizeof(pSql),
            "SELECT ZTRYBH, ZP FROM "
            "( "
            "   SELECT A.*, ROWNUM RN "
            "   FROM (SELECT * FROM %s) A "
            "   WHERE ROWNUM <= %d "
            ") "
            "WHERE RN > %d and BH like '%%_1' and SFZH is not NULL and ZP is not NULL and YWZT = '0'",
            NWORACLEZTYXTABLE, i + 1000, i);
        if (!m_DBMgr.ExecuteSQL(pSql, otlSelect))
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ִ��SQL���ʧ��!");
            getchar();
            return false;
        }
        else
        {
            g_LogRecorder.WriteDebugLogEx(__FUNCTION__, "ִ��SQL���, ��ȡOracle����������Ա��Ϣ��...������[%d].", i);
        }

        if (otlSelect.eof())
        {
            otlSelect.close();
            break;
        }

        while (!otlSelect.eof())
        {
            otlSelect >> ZTRYBH >> sZP;
            nLen = sZP.length;
            if (0 == nLen)
            {
                continue;
            }

            sTempPath = sZTYXPath + string(ZTRYBH) + string(".jpg");
            HANDLE hFileHandle = CreateFile(sTempPath.c_str(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                NULL,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                NULL);

            WriteFile(hFileHandle, sZP.v, nLen, &dwWrite, NULL);
            CloseHandle(hFileHandle);
        }
        otlSelect.close();
    }
    
    
    g_LogRecorder.WriteDebugLog(__FUNCTION__, "���º�����������Ϣ����!");
    return true;
}
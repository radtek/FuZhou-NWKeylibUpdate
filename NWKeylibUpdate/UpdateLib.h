#pragma once
#include "DataBaseMgr.h"
#include "mysql_acl.h"
#include "vector"
#include "ZBase64.h"
#include "hpsocket/hpsocket.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "ConfigRead.h"


#define NWORACLEZTYXTABLE   "v_ztryxx_and_zp"    //oracleԴ����ͼ��
#define MYSQLUPDATETABLE    "ga_ztryxx"         //���ݵ��ص���Ա��ϸ��Ϣ
#define MYSQLSTOREFACEINFO  "storefaceinfo"     //����ص���Ա��Ϣ��
#define MYSQLSTORECOUNT     "storecount"        //���ؿ�������
#define MYSQLLAYOUTRESULT   "layoutresult"      //Ԥ�������
#define ZTRYSTORELIBID          100                 //������Ա��ID
#define LOCALUPDATESERVERNAME "UpdateServerName"    //zmq���ı���������

#define JSONSTORELIBID  "StoreLibID"
#define JSONSTOREPHOTO  "Photo"
#define JSONLIBTYPE     "LibType"
#define JSONSTOREFACE   "StoreFace"
#define JSONFACEUUID    "FaceUUID"

#define MAXIMAGESIZE    1024 * 1024 * 10

typedef struct _UpdateDBInfo
{
    char pOracleDBIP[64];
    char pOracleDBName[64];
    char pOracleDBUser[64];
    char pOracleDBPassword[64];
    char pMysqlDBIP[64];
    int nMysqlDBPort;
    char pMysqlDBName[64];
    char pMysqlDBUser[64];
    char pMysqlDBPassword[64];

    char pSavePath[64];
    char pBatchStoreServerIP[64];
    int nBatchStoreServerPort;
}UPDATEDBINFO, *LPUPDATEDBINFO;

typedef struct _ZDRYInfo
{
    char pName[64];
    char pSex[64];
    char pSFZH[64];
    char pAddress[256];
    char pImagePath[128];
    _ZDRYInfo()
    {
        ZeroMemory(pName, sizeof(pName));
        ZeroMemory(pSex, sizeof(pSex));
        ZeroMemory(pSFZH, sizeof(pSFZH));
        ZeroMemory(pAddress, sizeof(pAddress));
        ZeroMemory(pImagePath, sizeof(pImagePath));
    }
}ZDRYINFO, *LPZDRYINFO;
typedef map<string, LPZDRYINFO> MAPZDRYINFO;

class CUpdateLib
{
public:
    CUpdateLib();
    ~CUpdateLib();
public:
    bool StartUpdate();
    bool StopUpdate();
private:
    //ͬ��Oracle����������Ϣ��Mysql
    bool SyncKeyLibInfo();
    //��������ͬ������ͼƬ�����ر���
    bool DownloadAddImage();
    //����������Ϣ���������ط���
    bool AddBatchInfo();
    //ɾ�����಼����Ϣ���������ط���
    bool DelBatchInfo();
    //���¿�������storecount��
    bool UpdateStoreCount();
    //ȫ������ͬ���������Ⲽ����Ϣ
    bool UpdateKeyLib();

    //����������Ϣ, Ϊ��һ�θ���׼��
    bool ClearInfo();
private:
    bool InsertAddInfoToDB(char * pMsg);
private:
    HANDLE m_hStopEvent;                //ֹͣ�¼�
    CConfigRead * m_pConfigRead;
    CDataBaseMgr m_DBMgr;               //Oracle���ݿ����
    CMysql_acl m_mysqltool;             //MySQL���ݿ����
     

    LPUPDATEDBINFO m_pDBInfo;           //���ݿ������Ϣ
    map<string, string> m_mapLayoutBH;     //Mysql����������Ա���, FaceUUID
    set<string> m_setga_ztryxx;         //ga_ztryxx��ı�ż���
    
    MAPZDRYINFO m_mapZDRYInfo;
    int m_nLayoutSuccess;       
    int m_nLayoutFailed;        //���ʧ��ͼƬ����
};

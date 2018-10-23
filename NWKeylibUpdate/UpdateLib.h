#pragma once
#include "DataBaseMgr.h"
#include "mysql_acl.h"
#include "vector"
#include "ZBase64.h"
#include "hpsocket/hpsocket.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "ConfigRead.h"


#define NWORACLEZTYXTABLE   "v_ztryxx_and_zp"    //oracle源库视图名
#define MYSQLUPDATETABLE    "ga_ztryxx"         //备份的重点人员详细信息
#define MYSQLSTOREFACEINFO  "storefaceinfo"     //入库重点人员信息表
#define MYSQLSTORECOUNT     "storecount"        //布控库数量表
#define MYSQLLAYOUTRESULT   "layoutresult"      //预警结果表
#define ZTRYSTORELIBID          100                 //在逃人员库ID
#define LOCALUPDATESERVERNAME "UpdateServerName"    //zmq订阅本服务名称

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
    //同步Oracle黑名单库信息到Mysql
    bool SyncKeyLibInfo();
    //下载新增同步人脸图片到本地保存
    bool DownloadAddImage();
    //新增布控信息到批量布控服务
    bool AddBatchInfo();
    //删除冗余布控信息到批量布控服务
    bool DelBatchInfo();
    //更新库数量到storecount表
    bool UpdateStoreCount();
    //全部重新同步黑名单库布控信息
    bool UpdateKeyLib();

    //清理所有信息, 为下一次更新准备
    bool ClearInfo();
private:
    bool InsertAddInfoToDB(char * pMsg);
private:
    HANDLE m_hStopEvent;                //停止事件
    CConfigRead * m_pConfigRead;
    CDataBaseMgr m_DBMgr;               //Oracle数据库操作
    CMysql_acl m_mysqltool;             //MySQL数据库操作
     

    LPUPDATEDBINFO m_pDBInfo;           //数据库参数信息
    map<string, string> m_mapLayoutBH;     //Mysql黑名单库人员编号, FaceUUID
    set<string> m_setga_ztryxx;         //ga_ztryxx里的编号集合
    
    MAPZDRYINFO m_mapZDRYInfo;
    int m_nLayoutSuccess;       
    int m_nLayoutFailed;        //入库失败图片数量
};


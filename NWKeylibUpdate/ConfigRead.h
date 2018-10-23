#pragma once
#include <string>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/typeof/typeof.hpp>

using namespace boost::property_tree;
using namespace std;

#pragma comment( lib, "XS_Encrypt.lib" )


class CConfigRead
{
public:
	CConfigRead(void);
public:
    ~CConfigRead(void);
public:
    string GetCurrentPath();
    bool ReadConfig();
public:
    string m_sCurrentPath;  //程序当前路径
    string m_sConfigFile;	//配置路径

    string m_sDBIP;		    //数据库IP
    int m_nDBPort;          //数据库端口
    string m_sDBName;		//数据库名
    string m_sDBUid;		//用户名
    string m_sDBPwd;		//用户密码

    string m_sServerCode;   //服务Code
    string m_sSavePath;     //图片保存磁盘
    int m_nQuality;         //低于此质量分数的重点库图片不入库

    string m_sOracleIP;
    string m_sOracleName;
    string m_sOracleUser;
    string m_sOraclePassword;

    string m_sBatchStoreServerIP;   //批量布控服务ID
    int m_nBatchStoreServerPort;    //批量布控服务Port
    
    int m_nWaitTime;                //多久循环一次
};

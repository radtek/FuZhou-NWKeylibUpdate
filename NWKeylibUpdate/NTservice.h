// ntservice.h
//
// Definitions for CNTService
//

#ifndef _NTSERVICE_H_
#define _NTSERVICE_H_
#include <Windows.h>
#include <Winsvc.h>
#include "ntservmsg.h" // Event message ids

#define SERVICE_CONTROL_USER 128

class CNTService
{
public:
    CNTService(const char* szServiceName);
    virtual ~CNTService();
    bool ParseStandardArgs(int argc, char* argv[]);
    bool IsInstalled();
    bool Install();
    bool Uninstall();
    void LogEvent(WORD wType, DWORD dwID,
                  const char* pszS1 = NULL,
                  const char* pszS2 = NULL,
                  const char* pszS3 = NULL);
    bool StartService();
    void SetStatus(DWORD dwState);
    bool Initialize();
    virtual void Run();
	virtual bool OnInit();
    virtual void OnStop();
    virtual void OnInterrogate();
    virtual void OnPause();
    virtual void OnContinue();
    virtual void OnShutdown();
    virtual BOOL OnUserControl(DWORD dwOpcode);
    void DebugMsg(const char* pszFormat, ...);
    
    // static member functions
    static void WINAPI ServiceMain(DWORD dwArgc, LPTSTR* lpszArgv);
    static void WINAPI Handler(DWORD dwOpcode);

	//检测设备后台软件是否已经退出
	//BOOL CheckEquSoftIsExit(int &IsExit,DWORD &EquSoftProcessesID);

	//强制杀除一个进程
	//BOOL TerminateEquSoft(DWORD dwProcessId);

    // data members
    char m_szServiceName[64];
	char m_szDisplayName[64];
    int m_iMajorVersion;
    int m_iMinorVersion;
    SERVICE_STATUS_HANDLE m_hServiceStatus;
    SERVICE_STATUS m_Status;
    BOOL m_bIsRunning;

    // static data
    static CNTService* m_pThis; // nasty hack to get object ptr

private:
    HANDLE m_hEventSource;

};

#endif // _NTSERVICE_H_

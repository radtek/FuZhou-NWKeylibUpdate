// NTService.cpp
#pragma once
#include "stdafx.h"
#include "NTService.h"
#include "updatelib.h"

#include <shellapi.h>

#pragma warning (disable:4800)

CNTService* CNTService::m_pThis = NULL;
CUpdateLib g_UpdateLib;




CNTService::CNTService(const char* szServiceName)
{
    // copy the address of the current object so we can access it from
    // the static member callback functions. 
    // WARNING: This limits the application to only one CNTService object. 
    m_pThis = this;
    
    // Set the default service name and version
    strncpy(m_szServiceName, szServiceName, sizeof(m_szServiceName)-1);
    m_iMajorVersion = 1;
    m_iMinorVersion = 0;
    m_hEventSource = NULL;

    // set up the initial service status 
    m_hServiceStatus			= NULL;
    m_Status.dwServiceType		= SERVICE_WIN32_OWN_PROCESS;
    m_Status.dwCurrentState		= SERVICE_STOPPED;
    m_Status.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    m_Status.dwWin32ExitCode	= 0;
    m_Status.dwServiceSpecificExitCode = 0;
    m_Status.dwCheckPoint		= 0;
    m_Status.dwWaitHint			= 0;
    m_bIsRunning				= false;
}

CNTService::~CNTService()
{
    //DebugMsg("CNTService::~CNTService()");
    if (m_hEventSource) {
        ::DeregisterEventSource(m_hEventSource);
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Default command line argument parsing

// Returns TRUE if it found an arg it recognised, FALSE if not
// Note: processing some arguments causes output to stdout to be generated.
bool CNTService::ParseStandardArgs(int argc, char* argv[])
{
    // 没有命令行参数则执行服务程序
    if (argc <= 1)
	{
		StartService();
		return true;
	}
    else if (_stricmp(argv[1], "-v") == 0) 
	{
        // 显示版本信息
        printf("%s Version %d.%d\n",
               m_szServiceName, m_iMajorVersion, m_iMinorVersion);
        printf("服务%s安装\n",
               IsInstalled() ? "已经" : "尚未");
    } 
	else if (_stricmp(argv[1], "-i") == 0) 
	{
        // 检查该服务是否已经安装
        if (IsInstalled())
		{
            printf("%s 服务当前已经安装 重新安装请先卸载\n", m_szServiceName);
        } 
		else 
		{
            // 安装服务
            if (Install()) 
			{
                printf("%s 服务安装成功\n", m_szServiceName);
            } 
			else 
			{
                printf("%s 服务安装失败. 异常码 %d\n", m_szServiceName, GetLastError());
				
            }
        }
    } 
	else if (_stricmp(argv[1], "-u") == 0) 
	{

        // 检查该服务是否已经安装
        if (!IsInstalled()) 
		{
            printf("%s 服务尚未被安装\n", m_szServiceName);
        } 
		else 
		{
            // 卸载服务
            if (Uninstall()) 
			{
                // 得到文件路径 显示手动删除文件提示信息
                char szFilePath[MAX_PATH];
                ::GetModuleFileName(NULL, szFilePath, sizeof(szFilePath));
                printf("%s 服务被卸载. (请自行删除服务运行程序文件 %s )\n",
                       m_szServiceName, szFilePath);
            } 
			else 
			{
                printf("不可以卸载服务 %s. 异常码 %d\n", m_szServiceName, GetLastError());
            }
        }
    }
	else
	{
		//返回 未处理合法参数
		printf("无法识别的命令参数! \n");
		return false;
	}
         
    // Don't recognise the args
    return true;
}

////////////////////////////////////////////////////////////////////////////////////////
// Install/uninstall routines

// Test if the service is currently installed
bool CNTService::IsInstalled()
{
    bool bResult = false;

    // Open the Service Control Manager
    SC_HANDLE hSCM = ::OpenSCManager(NULL,					// local machine
                                     NULL,					// ServicesActive database
                                     SC_MANAGER_ALL_ACCESS);// full access
    if (hSCM) 
	{

        // 尝试打开服务
        SC_HANDLE hService = ::OpenService(hSCM,
                                           m_szServiceName,
                                           SERVICE_QUERY_CONFIG);
        if (hService) 
		{
            bResult = true;
            ::CloseServiceHandle(hService);
        }

        ::CloseServiceHandle(hSCM);
    }
    
    return bResult;
}

bool CNTService::Install()
{
    // 打开服务控制管理器
    SC_HANDLE hSCM = ::OpenSCManager(NULL,					// local machine
                                     NULL,					// ServicesActive database
                                     SC_MANAGER_ALL_ACCESS);// full access
    if (!hSCM) return false;

    // Get the executable file path
	char szFilePath[MAX_PATH] ;
    ::GetModuleFileName(NULL, szFilePath, sizeof(szFilePath));
	SC_HANDLE hService=NULL;
		
	// 创建服务
	hService = ::CreateService(hSCM,
		m_szServiceName,
		m_szServiceName,
		SERVICE_ALL_ACCESS,
		SERVICE_WIN32_OWN_PROCESS,	// SERVICE_INTERACTIVE_PROCESS
		SERVICE_AUTO_START,			// SERVICE_DEMAND_START
		SERVICE_ERROR_NORMAL,
		szFilePath,
		NULL,
		NULL,
		NULL,						//"MSSQLSERVER" 依赖的服务名称
		NULL,
		NULL);		
		
    if (!hService) 
	{
        ::CloseServiceHandle(hSCM);
        return FALSE;
    }

    // make registry entries to support logging messages
    // Add the source name as a subkey under the Application
    // key in the EventLog service portion of the registry.
    char szKey[256];
    HKEY hKey = NULL;
    strcpy(szKey, "SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\");
    strcat(szKey, m_szServiceName);
    if (::RegCreateKey(HKEY_LOCAL_MACHINE, szKey, &hKey) != ERROR_SUCCESS) {
        ::CloseServiceHandle(hService);
        ::CloseServiceHandle(hSCM);
        return FALSE;
    }

    // Add the Event ID message-file name to the 'EventMessageFile' subkey.
    ::RegSetValueEx(hKey,
                    "EventMessageFile",
                    0,
                    REG_EXPAND_SZ, 
                    (CONST BYTE*)szFilePath,
                    (DWORD)strlen(szFilePath) + 1);     

    // Set the supported types flags.
    DWORD dwData = EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE | EVENTLOG_INFORMATION_TYPE;
    ::RegSetValueEx(hKey,
                    "TypesSupported",
                    0,
                    REG_DWORD,
                    (CONST BYTE*)&dwData,
                     sizeof(DWORD));
    ::RegCloseKey(hKey);

    LogEvent(EVENTLOG_INFORMATION_TYPE, EVMSG_INSTALLED, m_szServiceName);

    // tidy up
    ::CloseServiceHandle(hService);
    ::CloseServiceHandle(hSCM);

    return true;
}

bool CNTService::Uninstall()
{
	if (!IsInstalled())
		return true;
    // Open the Service Control Manager
    SC_HANDLE hSCM = ::OpenSCManager(NULL, // local machine
                                     NULL, // ServicesActive database
                                     SC_MANAGER_ALL_ACCESS); // full access
    if (!hSCM) return FALSE;

    bool bResult = false;
    SC_HANDLE hService = ::OpenService(hSCM,
                                       m_szServiceName,
                                       DELETE);
    if (hService) {
        if (::DeleteService(hService)) {
            LogEvent(EVENTLOG_INFORMATION_TYPE, EVMSG_REMOVED, m_szServiceName);
            bResult = TRUE;
        } else {
            LogEvent(EVENTLOG_ERROR_TYPE, EVMSG_NOTREMOVED, m_szServiceName);
        }
        ::CloseServiceHandle(hService);
    }
    
    ::CloseServiceHandle(hSCM);
    return bResult;
}

///////////////////////////////////////////////////////////////////////////////////////
// Logging functions

// This function makes an entry into the application event log
void CNTService::LogEvent(WORD wType, DWORD dwID,
                          const char* pszS1,
                          const char* pszS2,
                          const char* pszS3)
{
    const char* ps[3];
    ps[0] = pszS1;
    ps[1] = pszS2;
    ps[2] = pszS3;

    int iStr = 0;
    for (int i = 0; i < 3; i++) {
        if (ps[i] != NULL) iStr++;
    }
        
    // Check the event source has been registered and if
    // not then register it now
    if (!m_hEventSource) {
        m_hEventSource = ::RegisterEventSource(NULL,  // local machine
                                               m_szServiceName); // source name
    }

    if (m_hEventSource) {
        ::ReportEvent(m_hEventSource,
                      wType,
                      0,
                      dwID,
                      NULL, // sid
                      iStr,
                      0,
                      ps,
                      NULL);
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////
// Service startup and registration

bool CNTService::StartService()
{
	DebugMsg("Enter StartService...");

    SERVICE_TABLE_ENTRY st[] = {
        {m_szServiceName, ServiceMain},
        {NULL, NULL}
    };

    DebugMsg("Calling StartServiceCtrlDispatcher()...");
    bool b = ::StartServiceCtrlDispatcher(st);
	if (!b)
	{
		LPVOID lpMsgBuf;
		FormatMessage( 
			FORMAT_MESSAGE_ALLOCATE_BUFFER | 
			FORMAT_MESSAGE_FROM_SYSTEM | 
			FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
			(LPTSTR) &lpMsgBuf,
			0,
			NULL 
			);

		printf("异常: %s \n", lpMsgBuf );
		// Free the buffer.
		LocalFree( lpMsgBuf );
		Run();
		
	}
    DebugMsg("Returned from StartServiceCtrlDispatcher()");

	return b;
}

// static member function (callback)
void CNTService::ServiceMain(DWORD dwArgc, LPTSTR* lpszArgv)
{
    // Get a pointer to the C++ object

    CNTService* pService = m_pThis;//静态指针
    
    pService->DebugMsg("Entering CNTService::ServiceMain()");
    // Register the control request handler
    pService->m_Status.dwCurrentState = SERVICE_START_PENDING;
    pService->m_hServiceStatus = RegisterServiceCtrlHandler(pService->m_szServiceName,
                                                           Handler);
    if (pService->m_hServiceStatus == NULL) {
        pService->LogEvent(EVENTLOG_ERROR_TYPE, EVMSG_CTRLHANDLERNOTINSTALLED);
        return;
    }

    // 初始化
    if (pService->Initialize()) 
	{
        // 当RUN函数返回时 服务退出
        pService->m_bIsRunning				= TRUE;
        pService->m_Status.dwWin32ExitCode	= 0;
        pService->m_Status.dwCheckPoint		= 0;
        pService->m_Status.dwWaitHint		= 0;
        pService->Run();
    }

    // 通知服务管理 服务退出
    pService->SetStatus(SERVICE_STOPPED);


    pService->DebugMsg("Leaving CNTService::ServiceMain()");
}

///////////////////////////////////////////////////////////////////////////////////////////
// status functions

void CNTService::SetStatus(DWORD dwState)
{
    DebugMsg("CNTService::SetStatus(%lu, %lu)", m_hServiceStatus, dwState);
    m_Status.dwCurrentState = dwState;
    ::SetServiceStatus(m_hServiceStatus, &m_Status);
}

///////////////////////////////////////////////////////////////////////////////////////////
// Service initialization

bool CNTService::Initialize()
{
	
	//DebugMsg("Entering CNTService::Initialize()");

    // Start the initialization
    SetStatus(SERVICE_START_PENDING);
    
    // Perform the actual initialization
    bool bResult = OnInit(); 
    
    // Set final state
    m_Status.dwWin32ExitCode = GetLastError();
    m_Status.dwCheckPoint = 0;
    m_Status.dwWaitHint = 0;
    if (!bResult) {
        LogEvent(EVENTLOG_ERROR_TYPE, EVMSG_FAILEDINIT);
        SetStatus(SERVICE_STOPPED);
        return false;    
    }
    
    LogEvent(EVENTLOG_INFORMATION_TYPE, EVMSG_STARTED);
    SetStatus(SERVICE_RUNNING);

    //DebugMsg("Leaving CNTService::Initialize()");
    return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// main function to do the real work of the service

// This function performs the main work of the service. 
// When this function returns the service has stopped.
void CNTService::Run()
{
	DebugMsg("-----Entering CNTService::Run()");

    g_UpdateLib.StartUpdate();

	DebugMsg("-----out CNTService::Run()");

	DebugMsg("Leaving CNTService::Run()");
}

//////////////////////////////////////////////////////////////////////////////////////
// Control request handlers

// static member function (callback) to handle commands from the
// service control manager
void CNTService::Handler(DWORD dwOpcode)
{
    // Get a pointer to the object
    CNTService* pService = m_pThis;
    
    pService->DebugMsg("CNTService::Handler(%lu)", dwOpcode);
    switch (dwOpcode) {
    case SERVICE_CONTROL_STOP:		// 1
        pService->SetStatus(SERVICE_STOP_PENDING);
        pService->OnStop();
        pService->m_bIsRunning = false;
        pService->LogEvent(EVENTLOG_INFORMATION_TYPE, EVMSG_STOPPED);
        break;

    case SERVICE_CONTROL_PAUSE:		// 2
        pService->OnPause();
        break;

    case SERVICE_CONTROL_CONTINUE:	// 3
        pService->OnContinue();
        break;

    case SERVICE_CONTROL_INTERROGATE: // 4
        pService->OnInterrogate();
        break;

    case SERVICE_CONTROL_SHUTDOWN:	// 5
        pService->OnShutdown();
        break;

    default:
        if (dwOpcode >= SERVICE_CONTROL_USER) 
		{
            if (!pService->OnUserControl(dwOpcode)) 
			{
                pService->LogEvent(EVENTLOG_ERROR_TYPE, EVMSG_BADREQUEST);
            }
        } 
		else 
		{
            pService->LogEvent(EVENTLOG_ERROR_TYPE, EVMSG_BADREQUEST);
        }
        break;
    }

    // Report current status
    pService->DebugMsg("Updating status (%lu, %lu)",
                       pService->m_hServiceStatus,
                       pService->m_Status.dwCurrentState);
    ::SetServiceStatus(pService->m_hServiceStatus, &pService->m_Status);
}
        
// Called when the service is first initialized
bool CNTService::OnInit()
{
   
	// 此处可以添加服务初始化代码

	
	DebugMsg("CNTService::OnInit()");

	return true;
}

// Called when the service control manager wants to stop the service
void CNTService::OnStop()
{
	//此处添加服务被终止代码
    g_UpdateLib.StopUpdate();

	m_bIsRunning = false; //运行标志为false
	
	DebugMsg("CNTService::OnStop()");
}

// called when the service is interrogated
void CNTService::OnInterrogate()
{
    DebugMsg("CNTService::OnInterrogate()");
}

// called when the service is paused
void CNTService::OnPause()
{
    DebugMsg("CNTService::OnPause()");
}

// called when the service is continued
void CNTService::OnContinue()
{
    DebugMsg("CNTService::OnContinue()");
}

// called when the service is shut down
void CNTService::OnShutdown()
{
    DebugMsg("CNTService::OnShutdown()");
}

// called when the service gets a user control message
BOOL CNTService::OnUserControl(DWORD dwOpcode)
{
    DebugMsg("CNTService::OnUserControl(%8.8lXH)", dwOpcode);
    return FALSE; // say not handled
}


////////////////////////////////////////////////////////////////////////////////////////////
// Debugging support

void CNTService::DebugMsg(const char* pszFormat, ...)
{
    char buf[1024];
    sprintf(buf, "[%s](%lu): ", m_szServiceName, GetCurrentThreadId());
	va_list arglist;
	va_start(arglist, pszFormat);
    vsprintf(&buf[strlen(buf)], pszFormat, arglist);
	va_end(arglist);
    strcat(buf, "\n");
    OutputDebugString(buf);
}

/*
//强制杀除一个进程
BOOL CNTService::TerminateEquSoft(DWORD dwProcessId)
{
	//提升自身进程权限
	HANDLE hToken;
	TOKEN_PRIVILEGES tkp;
	LUID sedebugnameValue;
		
	BOOL succ = OpenProcessToken(GetCurrentProcess(),TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken);
	succ = LookupPrivilegeValue(NULL, SE_DEBUG_NAME,&sedebugnameValue);
		
	tkp.PrivilegeCount = 1;  // one privilege to set
	tkp.Privileges[0].Luid = sedebugnameValue;
	tkp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
		
	succ = AdjustTokenPrivileges(hToken, FALSE, &tkp, sizeof(TOKEN_PRIVILEGES),NULL, NULL);

	//杀除设备软件进程
	HANDLE hProcess = ::OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, dwProcessId);
	if (hProcess)
	{
		if(TerminateProcess(hProcess,-1))
		{
			return TRUE;
		}
		else
		{
			return FALSE;
		}
	}
	else
		return FALSE;

	
}
*/
/*
//检测设备后台软件是否已经退出
BOOL CNTService::CheckEquSoftIsExit(int &IsExit,DWORD &EquSoftProcessesID)
{
	BOOL (WINAPI *lpfEnumProcesses)(DWORD *, DWORD cb, DWORD *);
	DWORD (WINAPI *lpfGetModuleFileNameEx)(HANDLE, HMODULE, LPTSTR, DWORD);

	HINSTANCE hInstLib = ::LoadLibrary("PSAPI.DLL");
	if( hInstLib == NULL ) 
		return FALSE;

	lpfEnumProcesses = (BOOL (WINAPI *)(DWORD *,DWORD,DWORD*))GetProcAddress(hInstLib, "EnumProcesses");
	lpfGetModuleFileNameEx = (DWORD (WINAPI *)(HANDLE, HMODULE, LPTSTR, DWORD))GetProcAddress(hInstLib, "GetModuleFileNameExA");

	if(NULL == lpfEnumProcesses||NULL == lpfGetModuleFileNameEx)
		return FALSE;

	DWORD dwProcessesID[1024], dwNeeded, dwProcesses;
	HANDLE hProcess;
	char szProcessName[MAX_PATH];
	int j, nProcessID;

	::memset(dwProcessesID, 0, sizeof(DWORD) * 1024);
	if (!lpfEnumProcesses(dwProcessesID, 1024, &dwNeeded)) 
		return FALSE;

	dwProcesses = dwNeeded / sizeof(DWORD);
	nProcessID = 0;
	for(j = 0; j < (int)dwProcesses; j ++)
	{
		hProcess = ::OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, dwProcessesID[j]);
		if (hProcess)
		{
			::memset(szProcessName, 0, MAX_PATH);
			lpfGetModuleFileNameEx(hProcess, NULL, szProcessName, sizeof(szProcessName));
			if(szProcessName != NULL)
			{
				//如果找到匹配的设备软件可执行文件名则说明还在运行
				if(strstr(szProcessName,"ICTNetSystem.exe"))
				{
					::CloseHandle(hProcess );

					//表示程序还未退出
					IsExit = 0;

					//未退出则传出其进程号
					EquSoftProcessesID = dwProcessesID[j];

					return TRUE;
				}
			}
		}
		::CloseHandle(hProcess );
	}

	//表示程序已经退出
	IsExit = 1;

	return TRUE;
}
*/
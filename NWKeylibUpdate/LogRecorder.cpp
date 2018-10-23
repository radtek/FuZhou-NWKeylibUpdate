#include "stdafx.h"
#include "LogRecorder.h"
#include <stdio.h>


#pragma warning(disable : 4996)

CLogRecorder::CLogRecorder(void)
{
}

CLogRecorder::~CLogRecorder(void)
{
}

bool CLogRecorder::InitLogger(const char * pszLogConfigPath, const char * pszLoggerName, const char * pszModuleName)
{
	bool bRet = false;
	try
	{
        if (NULL == strstr(pszLogConfigPath, ".xml"))
            log4cxx::PropertyConfigurator::configure(pszLogConfigPath);
        else	// .xml配置文件，报异常，暂时不能解决          
			DOMConfigurator::configure(pszLogConfigPath);

		m_logger = Logger::getLogger(pszLoggerName);
		memset(m_szModuleName, 0, sizeof(m_szModuleName));
		_snprintf(m_szModuleName, sizeof(m_szModuleName), "%s", pszModuleName);

		bRet = true;
	}
	catch (...)
	{
		bRet = false;
		//LogLog::error(LOG4CXX_STR("Could not create the TriggeringPolicy. Reported error follows."), e);
		//LogLog::error((LogString&)("初始化Logger失败，请检查配置文件或配置路径"));
	}

	return bRet;
}

void CLogRecorder::WriteLog(const char * pszFunction, const char * pszLogContent, LoggerLevel nLevel/* =INFOLEVEL */)
{
	if (NULL == m_logger)
		return;

	char szLogRecord[1024*5] = {0};
	_snprintf(szLogRecord, sizeof(szLogRecord), "[%s] %s", pszFunction, pszLogContent);
    

	switch (nLevel)
	{
	case TRACELEVEL:
		//m_logger->trace(szLogRecord);
		LOG4CXX_TRACE(m_logger, szLogRecord);
		break;

	case DEBUGLEVEL:
		//m_logger->debug(szLogRecord);
		LOG4CXX_DEBUG(m_logger, szLogRecord);
		break;

	case INFOLEVEL:
		//m_logger->info(szLogRecord);
		LOG4CXX_INFO(m_logger, szLogRecord);
		break;

	case WARNLEVEL:
		//m_logger->warn(szLogRecord);
		LOG4CXX_WARN(m_logger, szLogRecord);
		break;

	case ERRORLEVEL:
		//m_logger->error(szLogRecord);
		LOG4CXX_ERROR(m_logger, szLogRecord);
		break;

	case FATALLEVEL:
		//m_logger->error(szLogRecord);
		LOG4CXX_FATAL(m_logger, szLogRecord);
		break;

	default:
		break;
	}
}

void CLogRecorder::WriteDebugLog(const char * pszFunction, const char * pszLogContent)
{
	WriteLog(pszFunction, pszLogContent, DEBUGLEVEL);
    printf(pszLogContent);
    printf("\n");
}

void CLogRecorder::WriteInfoLog(const char * pszFunction, const char * pszLogContent)
{
	WriteLog(pszFunction, pszLogContent, INFOLEVEL);
}

void CLogRecorder::WriteWarnLog(const char * pszFunction, const char * pszLogContent)
{
	WriteLog(pszFunction, pszLogContent, WARNLEVEL);
    printf(pszLogContent);
    printf("\n");
}

void CLogRecorder::WriteErrorLog(const char * pszFunction, const char * pszLogContent)
{
	WriteLog(pszFunction, pszLogContent, ERRORLEVEL);
    printf(pszLogContent);
    printf("\n");
}

void CLogRecorder::WriteFatalLog(const char * pszFunction, const char * pszLogContent)
{
	WriteLog(pszFunction, pszLogContent, FATALLEVEL);
}

void CLogRecorder::WriteLogEx(LoggerLevel nLevel, const char * pszFunction, const char * pszFormat, ...)
{
	va_list arlist;	// define a variable points to variable arguments
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteLog(pszFunction, szLogContent, nLevel);
}

void CLogRecorder::WriteDebugLogEx(const char * pszFunction,const char * pszFormat, ...)
{
	va_list arlist;
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteDebugLog(pszFunction, szLogContent);
}

void CLogRecorder::WriteInfoLogEx(const char * pszFunction,const char * pszFormat, ...)
{
	va_list arlist;
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteInfoLog(pszFunction, szLogContent);
}

void CLogRecorder::WriteWarnLogEx(const char * pszFunction,const char * pszFormat, ...)
{
	va_list arlist;
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteWarnLog(pszFunction, szLogContent);
}

void CLogRecorder::WriteErrorLogEx(const char * pszFunction,const char * pszFormat, ...)
{
	va_list arlist;
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteErrorLog(pszFunction, szLogContent);
}

void CLogRecorder::WriteFatalLogEx(const char * pszFunction,const char * pszFormat, ...)
{
	va_list arlist;
	va_start(arlist, pszFormat);
	char szLogContent[1024] = {0};
	_vsnprintf(szLogContent, sizeof(szLogContent), pszFormat, arlist);
	va_end(arlist);

	WriteFatalLog(pszFunction, szLogContent);
}
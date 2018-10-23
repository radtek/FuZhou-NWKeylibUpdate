// stdafx.h : 标准系统包含文件的包含文件，
// 或是经常使用但不常更改的
// 特定于项目的包含文件
//

#pragma once

#define WIN32_LEAN_AND_MEAN		// 从 Windows 头中排除极少使用的资料

#include <stdio.h>
#include <tchar.h>
#include <Windows.h>
#include <string>
#include <list>
#include <map>
#include <set>
#include "LogRecorder.h"
#pragma comment(lib, "HPSocket.lib")

// TODO:  在此处引用程序需要的其他头文件

#define MAXLEN      128
#define MAXIPLEN        20          //IP, FaceRect最大长度
#define FEATURELEN  4096
#define FEATUREMIXLEN   500         //Feature最短长度, 小于此长度定为不合法
#define THREADNUM  8           //获取特征值处理线程数
#define THREADWAITTIME 5



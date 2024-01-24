package osskv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	KV_INFOLOG_PATH   = "/home/wwlocal/log/osskv/"
	KV_PIPE_NAME      = KV_INFOLOG_PATH + "oss_kv_pipe_no_del_wwl"
	S_IFIFO           = 0010000
	S_IFMT            = 0170000
	g_iKVBaseNameLen  = 63
	STRING_TYPE       = 5
	g_uAutoKeyLogType = 3
)

var (
	g_OssLock           sync.Mutex
	g_iCurrenPid        = 0
	g_bUseHelperThread  = true
	g_iPipeCleanFlag    = 0
	g_uLocalHost        uint32
	g_bUseGlobalTimeNow bool
	g_iTimeNow          int64
	m_szBaseName        [g_iKVBaseNameLen + 1]byte
	g_szModName         [g_iKVBaseNameLen + 1]byte
	szTemp              [512]byte
	g_szBaseName        [64]byte
	lpPoint             *byte
	g_bTimerExitFlag    = false
	g_FlushPipeThreadID = 0
	g_iKVPipeFD         int
	g_szKVPipeBuffer    []byte
	strRecordBuffer     []byte
	g_iKVPipeBufferLen  = 0
	g_file              *os.File
)

type clsKvRecord struct {
	m_iLogType   uint32
	m_iUid       uint32
	m_iKey       uint32
	m_iHid       uint32
	m_iTime      uint32
	m_szBaseName string
	m_iValueType uint32
	m_iValueSize uint32
	m_Value      uint32
	m_sVal       string
	m_sStringKey string
	m_bFixed     bool
	m_iMid       uint32
	m_iFid       uint32
	m_iCount     uint32
}

func MakeKVFIFO(pszFifoName string) int {
	iRet := 0

	iMode := syscall.Umask(0)
	err := syscall.Mkfifo(pszFifoName, syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IRWXO)
	if err != nil {
		if _, err := os.Stat(KV_INFOLOG_PATH); os.IsNotExist(err) {
			if err := os.MkdirAll(KV_INFOLOG_PATH, 0777); err != nil {
				return 1
			}
			syscall.Mkfifo(pszFifoName, syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IRWXO)
		}
	}
	syscall.Umask(iMode)

	if iRet != 0 && err == syscall.EEXIST {
		iRet = 0
	}
	return iRet
}
func GetPipeDescriptor(fd int, bCreate bool) int {
	if fd > 0 {
		return -1
	}

	var iLastTime time.Time
	iNowTime := time.Now()
	if iNowTime.Sub(iLastTime) < 10*time.Second {
		return -1
	}
	iLastTime = iNowTime

	//signal(SIGPIPE, SIG_IGN)
	fd2, err := syscall.Open(KV_PIPE_NAME, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	fd = fd2
	fmt.Println("fd", fd)
	if err != nil {
		return fd
	}

	var st syscall.Stat_t
	ret := syscall.Fstat(fd, &st)
	if ret != nil {
		return fd
	}
	if st.Mode&S_IFMT != S_IFIFO {
		ret = syscall.Unlink(KV_PIPE_NAME)
		if ret != nil {
			fmt.Println("find not pipe file")
			fd = -1
		} else {
			return fd
		}
	} else {
		return fd
	}

	if !bCreate {
		return fd
	}

	//MakeKVFIFO(KV_PIPE_NAME)
	fd, err = syscall.Open(KV_PIPE_NAME, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	return fd
}

func StopKVHelperThread() {
	g_bTimerExitFlag = true
	g_bUseGlobalTimeNow = false
	g_bUseHelperThread = false
	g_OssLock.Lock()
	defer g_OssLock.Unlock()
	if g_FlushPipeThreadID > 0 && g_iCurrenPid == syscall.Getpid() {
		// 等待FlushPipeWorker结束
		// 这里使用time.Sleep模拟pthread_join的功能
		time.Sleep(100 * time.Millisecond)
		g_FlushPipeThreadID = 0
	}
}

func ChildAfterFork() {
	g_OssLock.Lock()
	defer g_OssLock.Unlock()

	// swap flag
	bUseGlobalTimeNow := g_bUseGlobalTimeNow
	bUseHelperThread := g_bUseHelperThread
	StopKVHelperThread()
	// assign back
	g_bUseGlobalTimeNow = bUseGlobalTimeNow
	g_bUseHelperThread = bUseHelperThread

	g_iKVPipeBufferLen = 0
	g_iKVPipeBufferLen = 0
	if g_iKVPipeFD > 0 {
		_ = syscall.Close(g_iKVPipeFD)
		g_iKVPipeFD = -1
	}
}
func GetGiKVPipeFD(g_iKVPipeFD *int) {
	filePath := "/home/wwlocal/log/osskv/oss_kv_pipe_no_del_wwl"
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
	}
	//defer file.Close()
	g_file = file

	// 获取文件描述符
	*g_iKVPipeFD = int(file.Fd())
}

func WriteKVLog2Pipe(buf []byte, buffer *[]byte) int {
	iLastTime := 0
	iCurTime := 0
	GetGiKVPipeFD(&g_iKVPipeFD)
	if g_iKVPipeFD <= 0 {
		return 11
	}
	if buf == nil {
		return 12
	}
	iRet := 0
	n := len(buf)
	if n > 4096 {
		return 13
	}
	if n+g_iKVPipeBufferLen < 4096 {
		// copy(g_szKVPipeBuffer[g_iKVPipeBufferLen:], buf)
		*buffer = append(*buffer, buf...)
		g_iKVPipeBufferLen += n
		if g_iKVPipeBufferLen < 4096 && (iCurTime-iLastTime) <= 1 {
			return 0
		}
	}
	iLastTime = iCurTime
	iRet = WritePiPe(&g_iKVPipeFD, buf, n, 1)
	fmt.Println("WriteKVLog2Pipe iRet", iRet)
	if iRet < 0 {
		return iRet
	}

	// time.Sleep(5 * time.Second)
	// readBuf := make([]byte, n)
	// iRet = ReadPiPe(&fd, readBuf, n, 1)
	// fmt.Println("buf1", readBuf)
	// ckr1 := Deserialize(readBuf)
	// fmt.Println("buf1: ", ckr1)
	return 0
}
func isPrivateAddr(addr uint32) bool {
	if (addr >= 157772160 && addr <= 184549375) || (addr >= 1681915904 && addr <= 1686110207) ||
		(addr >= 2886729728 && addr <= 2887778303) || (addr >= 3232235520 && addr <= 3232301055) {
		return true
	}
	//fmt.Println("addr", addr)
	return false
}
func GetPrivateAddrAsNumeric() uint32 {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 0
	}
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, _ := net.ParseCIDR(addr.String())
			//fmt.Println("ip", ip)
			if ipv4 := ip.To4(); ipv4 != nil {
				ipv4Int := uint32(ipv4[0])<<24 | uint32(ipv4[1])<<16 | uint32(ipv4[2])<<8 | uint32(ipv4[3])
				//fmt.Println("ipv4Int", ipv4Int)
				if !isPrivateAddr(ipv4Int) {
					continue
				}
				return ipv4Int
			}
		}
	}
	return 0
}
func SetBaseName(pBaseName string) string {
	if pBaseName != "" {
		s := pBaseName[:int(math.Min(float64(len(pBaseName)), float64(g_iKVBaseNameLen)))]
		return s

	}
	return ""
}

func strrchr(s []byte, c byte) *byte {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return &s[i]
		}
	}
	return nil
}
func GetBaseName(pName string) string {
	var s string
	if pName != "" {
		return pName
	}

	if g_szModName == [g_iKVBaseNameLen + 1]byte{} {
		s = pName[:int(math.Min(float64(len(pName)), float64(g_iKVBaseNameLen)))]
	}

	return s

}

func Serialize(record *clsKvRecord, m_szBaseName []byte) []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, record.m_iLogType)
	binary.Write(&buf, binary.LittleEndian, record.m_iUid)
	binary.Write(&buf, binary.LittleEndian, record.m_iKey)
	binary.Write(&buf, binary.LittleEndian, record.m_iHid)
	binary.Write(&buf, binary.LittleEndian, record.m_iTime)
	binary.Write(&buf, binary.LittleEndian, record.m_iValueType)
	binary.Write(&buf, binary.LittleEndian, record.m_iValueSize)
	buf.Write(m_szBaseName)
	if record.m_iValueType == 5 {
		buf.WriteString(record.m_sVal)
	} else {
		binary.Write(&buf, binary.LittleEndian, 8)
	}

	if 3 == record.m_iLogType {
		buf.WriteString(record.m_sStringKey)
	}
	// base64Encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
	// fmt.Println("base64Encoded: ", base64Encoded)

	// Perform MD5 hash
	// md5Value := fmt.Sprintf("%x", md5.Sum([]byte(buf.Bytes())))
	// fmt.Println("md5Value: ", md5Value)

	return buf.Bytes()
}
func Deserialize(data []byte) *clsKvRecord {
	var record clsKvRecord

	buf := bytes.NewBuffer(data)

	binary.Read(buf, binary.LittleEndian, &record.m_iLogType)
	binary.Read(buf, binary.LittleEndian, &record.m_iUid)
	binary.Read(buf, binary.LittleEndian, &record.m_iKey)
	binary.Read(buf, binary.LittleEndian, &record.m_iHid)
	binary.Read(buf, binary.LittleEndian, &record.m_iTime)
	binary.Read(buf, binary.LittleEndian, &record.m_iValueType)
	binary.Read(buf, binary.LittleEndian, &record.m_iValueSize)
	binary.Read(buf, binary.LittleEndian, &record.m_iMid)
	binary.Read(buf, binary.LittleEndian, &record.m_iFid)
	binary.Read(buf, binary.LittleEndian, &record.m_iCount)
	binary.Read(buf, binary.LittleEndian, &record.m_Value)
	//binary.Read(buf, binary.LittleEndian, &record.m_Value)
	record.m_szBaseName = string(buf.Next(len(data) - buf.Len()))
	return &record
}
func StartKVHelperThread() {
	g_iCurrenPid = os.Getpid()
	g_bUseHelperThread = true
	g_OssLock.Lock()
	defer g_OssLock.Unlock()
	if g_FlushPipeThreadID > 0 {
		return
	}
	g_bTimerExitFlag = false
	go FlushPipeWorker()
}
func WritePiPe(fd *int, vptr []byte, n int, iMaxRetry int) int {
	nleft := n
	var nwritten int
	var ptr []byte
	iRetry := 0

	if iMaxRetry < 0 {
		iMaxRetry = 1
	}
	if iMaxRetry > 128 {
		iMaxRetry = 128
	}

	ptr = vptr

	for nleft > 0 {
		var err error
		nwritten, err = syscall.Write(*fd, ptr)
		if err != nil {
			if err == syscall.EINTR {
				fmt.Printf("debug %s %d fd=%d ptr=%p nleft=%d nwritten=%d errno=%d err:%s\n", "WritePiPe", 0, *fd, ptr, nleft, nwritten, err, err.Error())
				nwritten = 0
				iRetry++
				if iRetry >= iMaxRetry {
					return -22
				}
				time.Sleep(500 * time.Microsecond)
				continue
			} else {
				if err == syscall.EAGAIN {
					_ = syscall.Close(*fd)
					*fd = -1
				}
				fmt.Printf("debug %s %d fd=%d ptr=%p nleft=%d nwritten=%d errno=%d err:%s\n", "WritePiPe", 0, *fd, ptr, nleft, nwritten, err, err.Error())
				iRetry++
				if iRetry >= iMaxRetry {
					return -21
				}
				time.Sleep(500 * time.Microsecond)
				continue
			}
		}
		nleft -= nwritten
		ptr = ptr[nwritten:]
	}
	return n
}
func ReadPiPe(fd *int, vptr []byte, n int, iMaxRetry int) int {
	nleft := n
	var nread int
	var ptr []byte
	iRetry := 0

	if iMaxRetry < 0 {
		iMaxRetry = 1
	}
	if iMaxRetry > 128 {
		iMaxRetry = 128
	}

	ptr = vptr

	for nleft > 0 {
		var err error
		nread, err = syscall.Read(*fd, ptr)
		if err != nil {
			if err == syscall.EINTR {
				fmt.Printf("debug %s %d fd=%d ptr=%p nleft=%d nread=%d errno=%d err:%s\n", "ReadPiPe", 0, *fd, ptr, nleft, nread, err, err.Error())
				nread = 0
				iRetry++
				if iRetry >= iMaxRetry {
					return -22
				}
				time.Sleep(500 * time.Microsecond)
				continue
			} else {
				if err == syscall.EAGAIN {
					_ = syscall.Close(*fd)
					*fd = -1
				}
				fmt.Printf("debug %s %d fd=%d ptr=%p nleft=%d nread=%d errno=%d err:%s\n", "ReadPiPe", 0, *fd, ptr, nleft, nread, err, err.Error())
				iRetry++
				if iRetry >= iMaxRetry {
					return -21
				}
				time.Sleep(500 * time.Microsecond)
				continue
			}
		}
		nleft -= nread
		ptr = ptr[nread:]
	}
	return n
}

func FlushPipeWorker() {
	for !g_bTimerExitFlag {
		g_iTimeNow = time.Now().Unix()
		// Oss::Sleep2( 1 );
		time.Sleep(10 * time.Millisecond)
		g_OssLock.Lock()

		// 假设GetPipeDescriptor、WritePiPe等函数已经在Go中实现
		GetPipeDescriptor(g_iKVPipeFD, false)
		if g_iKVPipeFD <= 0 {
			g_OssLock.Unlock()
			continue
		}
		if g_iKVPipeBufferLen <= 0 {
			g_OssLock.Unlock()
			continue
		}

		iRet := WritePiPe(&g_iKVPipeFD, g_szKVPipeBuffer, g_iKVPipeBufferLen, 1)
		if iRet < 0 {
			g_OssLock.Unlock()
			continue
		}
		g_iKVPipeBufferLen = 0
		g_OssLock.Unlock()
	}
}
func FlushPipeBufWhenExit() {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("Exiting the program, performing cleanup...")
	g_bTimerExitFlag = true
	g_file.Close()

}
func startKVHelperThread(buffer *[]byte) {
	g_iCurrenPid = os.Getpid()
	g_bUseHelperThread = true
	g_bTimerExitFlag = false
	for !g_bTimerExitFlag {
		time.Sleep(10 * time.Millisecond)
		n := len(*buffer)
		if n > 0 {
			iRet := 0
			iRet = WritePiPe(&g_iKVPipeFD, *buffer, n, 1)
			fmt.Println("processBuffer iRet", iRet)
			*buffer = nil
			g_iKVPipeBufferLen = 0
		}
	}
}

// 在程序退出时调用的函数
func atexit(f func()) {
	go func() {
		select {
		// case <-os.After(0):
		// 	f()
		}
	}()
}
func OssKVLogImplementStart(v_iUid, v_iKey, v_iLogType int) {
	var iRet int
	var oRecord clsKvRecord
	var uHost uint32
	pHostIP := ""
	iUid := uint32(v_iUid)
	iKey := uint32(v_iKey)
	//线程开始时开启
	if (0 == g_iCurrenPid) && g_bUseHelperThread {
		//fmt.Println("g_iCurrenPid ", g_iCurrenPid)
		go startKVHelperThread(&strRecordBuffer)
	}
	//defer FlushPipeBufWhenExit()

	// g_OssLock.Lock()
	// defer g_OssLock.Unlock()

	// if g_iCurrenPid == 0 && g_bUseHelperThread {
	// 	StartKVHelperThread()
	// }
	if g_iPipeCleanFlag == 0 {
		g_iPipeCleanFlag = 1
		//fmt.Println("g_iPipeCleanFlag", g_iPipeCleanFlag)
		atexit(func() {
			fmt.Println("Exiting the program, performing cleanup...")
			g_bTimerExitFlag = true
			g_bUseHelperThread = false
			g_file.Close()
		})
	}
	if g_uLocalHost == 0 {
		g_uLocalHost = GetPrivateAddrAsNumeric()
		//fmt.Println("g_uLocalHost", g_uLocalHost)
	}
	if pHostIP == "" {
		uHost = g_uLocalHost
		//fmt.Println("uHost", uHost)
	} else {
		// if pAppName == nil {
		// 	return 6
		// }
		// Oss.IP2Int(*pHostIP, &uHost)
	}

	if uHost == 0 {
		return
	}
	oRecord.m_iLogType = uint32(v_iLogType)
	oRecord.m_iUid = iUid
	oRecord.m_iKey = iKey
	oRecord.m_iHid = uHost
	oRecord.m_iTime = uint32(time.Now().Unix())
	//oRecord.m_szBaseName = "wwlossagent"
	m_szBaseName := make([]byte, 72)
	copy(m_szBaseName, "wwlossagent")
	oRecord.m_iValueType = uint32(4)
	oRecord.m_iValueSize = uint32(8)
	if g_uAutoKeyLogType == oRecord.m_iLogType {
		oRecord.m_sStringKey = "ww0a7703daabb8c74e1"
		oRecord.m_iKey = uint32(len(oRecord.m_sStringKey))
	}
	b2 := Serialize(&oRecord, m_szBaseName)
	fmt.Println("oRecord: ", oRecord)

	// ckr := Deserialize(b2)
	// i := len(b2)
	// fmt.Println("i: ", i)
	// fmt.Println("ckr: ", ckr)
	//写通道数据
	iRet = WriteKVLog2Pipe(b2, &strRecordBuffer)
	fmt.Println("iRet", iRet)
}

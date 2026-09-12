package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

const executableName = "openabstractions.exe"

// PROC_THREAD_ATTRIBUTE_JOB_LIST is documented for Windows10/Server2016 onward.
// x/sys does not currently name it. Assignment is part of CreateProcess, before
// user code executes; unsupported hosts fail without a suspended-child fallback.
const attributeJobList = 0x0002000d

var queryJobProc = windows.NewLazySystemDLL("kernel32.dll").NewProc("QueryInformationJobObject")
var setJobProc = windows.NewLazySystemDLL("kernel32.dll").NewProc("SetInformationJobObject")

// Keep the pointer typed until LazyProc.Call's uintptr-escape boundary. The
// x/sys uintptr buffer signature can leave a movable Go stack address exposed.
func queryJob(job windows.Handle, class int32, data unsafe.Pointer, size uint32) error {
	result, _, err := queryJobProc.Call(uintptr(job), uintptr(class), uintptr(data), uintptr(size), 0)
	if result == 0 {
		return err
	}
	return nil
}

func setJob(job windows.Handle, class int32, data unsafe.Pointer, size uint32) error {
	result, _, err := setJobProc.Call(uintptr(job), uintptr(class), uintptr(data), uintptr(size))
	if result == 0 {
		return err
	}
	return nil
}

type windowsProcess struct {
	handle, job         windows.Handle
	id                  int
	budget              time.Duration
	mu                  sync.Mutex
	closed              bool
	terminating         bool
	members             []windows.Handle
	terminationErr      error
	terminationDeadline time.Time
	capturedTotal       uint32
}

func (p *windowsProcess) pid() int { return p.id }
func (p *windowsProcess) terminate() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if !p.terminating {
		p.terminating = true
		p.terminationDeadline = time.Now().Add(p.budget)
		members, err := p.memberHandles()
		p.members = members
		p.terminationErr = errors.Join(err, windows.TerminateJobObject(p.job, 1))
	}
	return p.terminationErr
}

// Capture waitable handles before requesting termination: the active-process
// count may reach zero before process objects become signaled.
func (p *windowsProcess) memberHandles() ([]windows.Handle, error) {
	var result []windows.Handle
	var before jobAccounting
	if err := queryJob(p.job, windows.JobObjectBasicAccountingInformation, unsafe.Pointer(&before), uint32(unsafe.Sizeof(before))); err != nil {
		return nil, err
	}
	p.capturedTotal = before.TotalProcesses
	for capacity := 16; capacity <= 1<<20; capacity *= 2 {
		if time.Now().After(p.terminationDeadline) {
			return result, errors.New("runtimehost: member observation deadline")
		}
		data := make([]uintptr, capacity+2)
		err := queryJob(p.job, windows.JobObjectBasicProcessIdList, unsafe.Pointer(&data[0]), uint32(8+capacity*int(unsafe.Sizeof(uintptr(0)))))
		if errors.Is(err, windows.ERROR_MORE_DATA) {
			continue
		}
		if err != nil {
			return nil, err
		}
		counts := (*[2]uint32)(unsafe.Pointer(&data[0]))
		ids := unsafe.Slice((*uintptr)(unsafe.Add(unsafe.Pointer(&data[0]), 8)), int(counts[1]))
		for _, id := range ids {
			if time.Now().After(p.terminationDeadline) {
				return result, errors.New("runtimehost: member observation deadline")
			}
			h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(id))
			if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
				continue
			} // Already exited and released.
			if err != nil {
				return result, err
			}
			result = append(result, h)
		}
		return result, nil
	}
	return nil, errors.New("runtimehost: job process list exceeds observation bound")
}
func (p *windowsProcess) closeJob() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		windows.CloseHandle(p.job)
		for _, h := range p.members {
			windows.CloseHandle(h)
		}
		p.closed = true
	}
}

// Native JOBOBJECT_BASIC_ACCOUNTING_INFORMATION has four LARGE_INTEGERs and
// four DWORDs. Querying ActiveProcesses observes descendant termination.
type jobAccounting struct {
	UserTime, KernelTime, UserPeriod, KernelPeriod                   int64
	PageFaults, TotalProcesses, ActiveProcesses, TerminatedProcesses uint32
}

func (p *windowsProcess) wait() error {
	defer windows.CloseHandle(p.handle)
	defer p.closeJob()
	if _, err := windows.WaitForSingleObject(p.handle, windows.INFINITE); err != nil {
		return err
	}
	var code uint32
	if err := windows.GetExitCodeProcess(p.handle, &code); err != nil {
		return err
	}
	if err := p.terminate(); err != nil {
		return fmt.Errorf("runtimehost: terminate remaining descendants: %w", err)
	}
	deadline := p.terminationDeadline
	for {
		var accounting jobAccounting
		if err := queryJob(p.job, windows.JobObjectBasicAccountingInformation, unsafe.Pointer(&accounting), uint32(unsafe.Sizeof(accounting))); err != nil {
			return err
		}
		if accounting.TotalProcesses != p.capturedTotal {
			return fmt.Errorf("runtimehost: job membership changed during termination (%d -> %d); complete signaling unproven", p.capturedTotal, accounting.TotalProcesses)
		}
		if accounting.ActiveProcesses == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("runtimehost: descendants remain after force deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, member := range p.members {
		remaining := time.Until(deadline)
		if remaining < 0 {
			return errors.New("runtimehost: process signals exceeded force deadline")
		}
		ms := uint32((remaining + time.Millisecond - 1) / time.Millisecond)
		status, err := windows.WaitForSingleObject(member, ms)
		if err != nil {
			return err
		}
		if status != windows.WAIT_OBJECT_0 {
			return errors.New("runtimehost: process termination not signaled within force deadline")
		}
	}
	if code != 0 {
		return fmt.Errorf("runtimehost: child exit code %d", code)
	}
	return nil
}
func launch(ctx context.Context, o Options) (_ process, _ *os.File, _ *os.File, err error) {
	if o.Stderr == nil {
		diagnostic, openErr := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if openErr != nil {
			return nil, nil, nil, openErr
		}
		defer diagnostic.Close()
		o.Stderr = diagnostic
	}
	if _, statErr := o.Stderr.Stat(); statErr != nil {
		return nil, nil, nil, fmt.Errorf("runtimehost stderr: %w", statErr)
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	success := false
	defer func() {
		if !success {
			windows.CloseHandle(job)
		}
	}()
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if err = setJob(job, windows.JobObjectExtendedLimitInformation, unsafe.Pointer(&limits), uint32(unsafe.Sizeof(limits))); err != nil {
		return nil, nil, nil, err
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	defer inR.Close()
	defer func() {
		if !success {
			inW.Close()
		}
	}()
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	defer outW.Close()
	defer func() {
		if !success {
			outR.Close()
		}
	}()
	// Only child standard handles are inheritable. Neither the sole writer nor
	// the unnamed job handle appears in the HANDLE_LIST.
	handles := []windows.Handle{windows.Handle(inR.Fd()), windows.Handle(outW.Fd()), 0}
	if err = windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(o.Stderr.Fd()), windows.CurrentProcess(), &handles[2], 0, true, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, nil, nil, err
	}
	defer windows.CloseHandle(handles[2])
	for _, h := range handles[:2] {
		if err = windows.SetHandleInformation(h, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
			return nil, nil, nil, err
		}
	}
	attrs, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return nil, nil, nil, err
	}
	defer attrs.Delete()
	if err = attrs.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&handles[0]), uintptr(len(handles))*unsafe.Sizeof(handles[0])); err != nil {
		return nil, nil, nil, err
	}
	if err = attrs.Update(attributeJobList, unsafe.Pointer(&job), unsafe.Sizeof(job)); err != nil {
		return nil, nil, nil, fmt.Errorf("runtimehost: atomic job assignment: %w", err)
	}
	si := windows.StartupInfoEx{StartupInfo: windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfoEx{})), Flags: windows.STARTF_USESTDHANDLES, StdInput: handles[0], StdOutput: handles[1], StdErr: handles[2]}, ProcThreadAttributeList: attrs.List()}
	exe, err := windows.UTF16PtrFromString(o.Executable)
	if err != nil {
		return nil, nil, nil, err
	}
	args := append([]string{o.Executable, "serve", "runtime", "--supervised"}, o.Args...)
	command, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(args))
	if err != nil {
		return nil, nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	var pi windows.ProcessInformation
	err = windows.CreateProcess(exe, command, nil, nil, true, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_NO_WINDOW, nil, nil, &si.StartupInfo, &pi)
	runtime.KeepAlive(handles)
	runtime.KeepAlive(job)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("runtimehost: atomic contained creation: %w", err)
	}
	windows.CloseHandle(pi.Thread)
	success = true
	return &windowsProcess{handle: pi.Process, job: job, id: int(pi.ProcessId), budget: o.ForceTimeout}, inW, outR, nil
}

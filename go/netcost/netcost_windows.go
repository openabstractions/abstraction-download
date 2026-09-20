//go:build windows

package netcost

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// NLM connection cost flags, netlistmgr.idl NLM_CONNECTION_COST.
const (
	costUnrestricted  = 0x1
	costFixed         = 0x2
	costVariable      = 0x4
	costOverDataLimit = 0x10000
	costRoaming       = 0x40000
)

var (
	clsidNetworkListManager      = windows.GUID{Data1: 0xDCB00C01, Data2: 0x570F, Data3: 0x4A9B, Data4: [8]byte{0x8D, 0x69, 0x19, 0x9F, 0xDB, 0xA5, 0x72, 0x3B}}
	iidINetworkCostManager       = windows.GUID{Data1: 0xDCB00008, Data2: 0x570F, Data3: 0x4A9B, Data4: [8]byte{0x8D, 0x69, 0x19, 0x9F, 0xDB, 0xA5, 0x72, 0x3B}}
	iidINetworkCostManagerEvents = windows.GUID{Data1: 0xDCB00009, Data2: 0x570F, Data3: 0x4A9B, Data4: [8]byte{0x8D, 0x69, 0x19, 0x9F, 0xDB, 0xA5, 0x72, 0x3B}}
	iidIConnectionPointContainer = windows.GUID{Data1: 0xB196B284, Data2: 0xBAB4, Data3: 0x101A, Data4: [8]byte{0xB6, 0x9C, 0x00, 0xAA, 0x00, 0x34, 0x1D, 0x07}}
	iidIUnknown                  = windows.GUID{Data4: [8]byte{0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46}}
	procCoCreateInstance         = windows.NewLazySystemDLL("ole32.dll").NewProc("CoCreateInstance")
)

const (
	vtQueryInterface      = 0
	vtRelease             = 2
	vtGetCost             = 3 // INetworkCostManager::GetCost
	vtFindConnectionPoint = 4 // IConnectionPointContainer::FindConnectionPoint
	vtAdvise              = 5 // IConnectionPoint::Advise
	vtUnadvise            = 6 // IConnectionPoint::Unadvise
	clsctxAll             = 0x17
	eNoInterface          = 0x80004002
)

// classify reads NLM cost flags. A fixed or variable plan, roaming or a plan
// over its limit is metered; unrestricted is unmetered; no flag is unknown.
func classify(flags uint32) Cost {
	switch {
	case flags&(costFixed|costVariable|costOverDataLimit|costRoaming) != 0:
		return Metered
	case flags&costUnrestricted != 0:
		return Unmetered
	}
	return Unknown
}

// comObject is any COM interface pointer: a pointer to its vtable.
type comObject struct{ vtbl *[8]uintptr }

func (o *comObject) call(slot int, args ...uintptr) uintptr {
	r, _, _ := syscall.SyscallN(o.vtbl[slot], append([]uintptr{uintptr(unsafe.Pointer(o))}, args...)...)
	return r
}

func failed(hr uintptr) bool { return int32(hr) < 0 }

// sinkVtable is INetworkCostManagerEvents: IUnknown, CostChanged,
// DataPlanStatusChanged. The callbacks are created once for the process.
var (
	sinkVtable  [8]uintptr
	sinkOnce    sync.Once
	sinkTargets sync.Map // sink object address -> *hub
)

func buildSinkVtable() {
	sinkVtable[0] = windows.NewCallback(func(this uintptr, riid *windows.GUID, out *uintptr) uintptr {
		if *riid == iidIUnknown || *riid == iidINetworkCostManagerEvents {
			*out = this
			return 0
		}
		*out = 0
		return eNoInterface
	})
	// The object's lifetime is the source's, so reference counts are fixed.
	sinkVtable[1] = windows.NewCallback(func(this uintptr) uintptr { return 2 })
	sinkVtable[2] = windows.NewCallback(func(this uintptr) uintptr { return 1 })
	sinkVtable[3] = windows.NewCallback(func(this, cost, dest uintptr) uintptr {
		// A null destination is the machine-wide cost, the only one asked for.
		if dest == 0 {
			if h, ok := sinkTargets.Load(this); ok {
				h.(*hub).set(classify(uint32(cost)))
			}
		}
		return 0
	})
	sinkVtable[4] = windows.NewCallback(func(this, dest uintptr) uintptr { return 0 })
}

// nlm is the Windows source. One locked OS thread owns the multithreaded COM
// apartment, the cost manager and the event connection for the source's life;
// NLM delivers CostChanged on its own RPC threads.
type nlm struct {
	*hub
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func open() (Source, error) {
	n := &nlm{stop: make(chan struct{}), done: make(chan struct{})}
	started := make(chan error, 1)
	go n.run(started)
	if err := <-started; err != nil {
		return nil, err
	}
	return n, nil
}

func (n *nlm) run(started chan<- error) {
	defer close(n.done)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := windows.CoInitializeEx(0, windows.COINIT_MULTITHREADED); err != nil {
		started <- fmt.Errorf("%w: CoInitializeEx: %v", ErrUnavailable, err)
		return
	}
	defer windows.CoUninitialize()

	var manager *comObject
	hr, _, _ := procCoCreateInstance.Call(uintptr(unsafe.Pointer(&clsidNetworkListManager)), 0, clsctxAll,
		uintptr(unsafe.Pointer(&iidINetworkCostManager)), uintptr(unsafe.Pointer(&manager)))
	if failed(hr) || manager == nil {
		started <- fmt.Errorf("%w: Network List Manager: HRESULT 0x%08x", ErrUnavailable, uint32(hr))
		return
	}
	defer manager.call(vtRelease)
	var flags uint32
	if hr := manager.call(vtGetCost, uintptr(unsafe.Pointer(&flags)), 0); failed(hr) {
		started <- fmt.Errorf("%w: INetworkCostManager::GetCost: HRESULT 0x%08x", ErrUnavailable, uint32(hr))
		return
	}
	n.hub = newHub(classify(flags))

	var container *comObject
	if hr := manager.call(vtQueryInterface, uintptr(unsafe.Pointer(&iidIConnectionPointContainer)), uintptr(unsafe.Pointer(&container))); failed(hr) {
		started <- fmt.Errorf("%w: IConnectionPointContainer: HRESULT 0x%08x", ErrUnavailable, uint32(hr))
		return
	}
	defer container.call(vtRelease)
	var point *comObject
	if hr := container.call(vtFindConnectionPoint, uintptr(unsafe.Pointer(&iidINetworkCostManagerEvents)), uintptr(unsafe.Pointer(&point))); failed(hr) {
		started <- fmt.Errorf("%w: cost events: HRESULT 0x%08x", ErrUnavailable, uint32(hr))
		return
	}
	defer point.call(vtRelease)

	sinkOnce.Do(buildSinkVtable)
	// COM holds this pointer, so it is pinned for as long as it is advised.
	sink := &comObject{vtbl: &sinkVtable}
	var pin runtime.Pinner
	pin.Pin(sink)
	defer pin.Unpin()
	address := uintptr(unsafe.Pointer(sink))
	sinkTargets.Store(address, n.hub)
	defer sinkTargets.Delete(address)
	var cookie uint32
	if hr := point.call(vtAdvise, address, uintptr(unsafe.Pointer(&cookie))); failed(hr) {
		started <- fmt.Errorf("%w: Advise: HRESULT 0x%08x", ErrUnavailable, uint32(hr))
		return
	}
	// A change between the first GetCost and Advise would otherwise be lost.
	if hr := manager.call(vtGetCost, uintptr(unsafe.Pointer(&flags)), 0); !failed(hr) {
		n.hub.set(classify(flags))
	}
	started <- nil

	<-n.stop
	point.call(vtUnadvise, uintptr(cookie))
	n.hub.close()
}

func (n *nlm) Close() error {
	n.once.Do(func() { close(n.stop) })
	<-n.done
	return nil
}

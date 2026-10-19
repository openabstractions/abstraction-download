package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	"github.com/openabstractions/abstraction-download/go/netcost"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
	resourceclient "github.com/openabstractions/abstraction-resource/go/client"
	resourceservice "github.com/openabstractions/abstraction-resource/go/service"
)

// wakeEndpoints numbers this process's wake fixture endpoints.
var wakeEndpoints atomic.Int64

func TestDownloadsKeepOnlyTheirAuthenticatedOwnersAwake(t *testing.T) {
	me, err := identity.Self()
	if err != nil {
		t.Fatal(err)
	}
	account, err := me.Account()
	if err != nil {
		t.Fatal(err)
	}
	makeBinding := func(name string) acceptanceprovider.Binding {
		program := identity.CanonicalProgramPath(filepath.Join(t.TempDir(), name))
		scope, e := acceptanceprovider.LocalSubjectScope(me.Kind, account, program)
		if e != nil {
			t.Fatal(e)
		}
		return acceptanceprovider.Binding{Scope: scope, Origin: "local", Subject: &acceptanceprovider.AuthenticatedSubject{AccountKind: me.Kind, Account: account, Executable: program, Program: program}}
	}
	a, b := makeBinding("app-a.exe"), makeBinding("app-b.exe")
	var mu sync.RWMutex
	permit := map[string]bool{a.Subject.Program: true, b.Subject.Program: true}
	var taken, released atomic.Int32
	table := resourceservice.NewTable(nil, nil, 0)
	book, err := resourceservice.OpenBook(resourceservice.BookOptions{Table: table,
		Hold: func(context.Context, *identity.Peer, string, string) error {
			return errors.New("runtime has no wake grant")
		},
		Standing: func(_ context.Context, s resourceservice.Subject, action, resource string) error {
			mu.RLock()
			defer mu.RUnlock()
			if s.Account != account || !permit[s.Program] || action != resourceservice.ActionHold || resource != resourceservice.ResourceAwake {
				return errors.New("not granted")
			}
			return nil
		},
		KeepSystemAwake: func(string) (func(), error) { taken.Add(1); return func() { released.Add(1) }, nil }})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listen.Endpoint(fmt.Sprintf("download-wake-%d-%d", os.Getpid(), wakeEndpoints.Add(1)))
	if err := listen.CanEver(endpoint, resourceservice.Bound); err != nil {
		t.Skip(err)
	}
	host, err := resourceservice.Listen(endpoint, table)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.EnableLeases(book); err != nil {
		t.Fatal(err)
	}
	if err := host.EnableEnforcer(func(_ context.Context, p *identity.Peer, action, resource string) bool {
		process, e := p.Process.AtLeast(identity.ProofKernel)
		return e == nil && process.PID == os.Getpid() && !process.Recycled && action == resourceservice.ActionHold && resource == resourceservice.ResourceAwake
	}); err != nil {
		t.Fatal(err)
	}
	resourceCtx, stopResource := context.WithCancel(context.Background())
	resourceDone := make(chan error, 1)
	go func() { resourceDone <- host.Serve(resourceCtx) }()
	defer func() { stopResource(); host.Close(); <-resourceDone }()
	t.Setenv(resourceclient.EnvEndpoint, endpoint)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer origin.Close()
	fastService(t)
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "wake-owner", HTTPExecution{NetworkCost: func() (netcost.Source, error) { return nil, errors.New("cost not required") }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	// Two claimants exercise overlapping accepted work without changing the
	// production runner's scheduling policy. The store fences each claim.
	for range 2 {
		go func() { done <- p.Execute(ctx) }()
	}
	defer func() {
		cancel()
		for range 2 {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("download executor did not stop")
			}
		}
		origin.CloseClientConnections()
	}()
	submit := func(binding acceptanceprovider.Binding, key string) api.RequestIdentity {
		bound := p.BindBinding(binding)
		window, e := bound.GetHistoryWindow()
		if e != nil {
			t.Fatal(e)
		}
		raw, e := json.Marshal(map[string]any{"artifact": map[string]any{}, "sources": []map[string]string{{"scheme": "http", "locator": origin.URL + "/" + key}}})
		if e != nil {
			t.Fatal(e)
		}
		id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}
		accepted, e := bound.Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: raw})
		if e != nil || accepted.Outcome != api.AcceptanceOutcomeAccepted {
			t.Fatal(accepted, e)
		}
		return id
	}
	submit(a, "download-a")
	idB := submit(b, "download-b")
	waitFor(t, "two original-owner wake leases", 5*time.Second, func() bool { return len(book.AwakeHolds()) == 2 })
	for _, row := range book.AwakeHolds() {
		if row.Account != account || row.Program != a.Subject.Program && row.Program != b.Subject.Program {
			t.Fatal("daemon principal substituted", row)
		}
	}
	mu.Lock()
	permit[a.Subject.Program] = false
	mu.Unlock()
	book.Recheck(context.Background())
	rows := book.AwakeHolds()
	if len(rows) != 1 || rows[0].Program != b.Subject.Program || released.Load() != 0 {
		t.Fatal("A revoke changed B wake", rows, released.Load())
	}
	if result, e := p.BindBinding(b).CancelWork(idB); e != nil || result.Outcome.String() != "requested" {
		t.Fatal(result, e)
	}
	waitFor(t, "final B wake released", 5*time.Second, func() bool { return len(book.AwakeHolds()) == 0 && released.Load() == 1 })
	if taken.Load() != 1 {
		t.Fatal("downloads took duplicate system requests", taken.Load())
	}
}

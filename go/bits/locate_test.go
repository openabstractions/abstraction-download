package bits

import (
	"context"
	"fmt"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
)

// The facade discovers this by type assertion, so a delegate that silently is
// not a Locator gets its lost handoffs treated as refusals again — and the
// answer to a refusal is to fetch the artifact here, alongside the one BITS is
// already fetching.
func TestDelegatorIsALocator(t *testing.T) {
	var d any = New()
	if _, ok := d.(download.Locator); !ok {
		t.Fatal("bits.Delegator does not implement download.Locator")
	}
}

func TestLocateRefusesAnEmptyRequest(t *testing.T) {
	if _, err := New().Locate(context.Background(), ""); err == nil {
		t.Fatal("an empty request matched something")
	}
}

// A request BITS accepted must be findable by the identity the caller minted,
// with nothing but the job queue to go on — no process, no memory, no handle.
func TestLocateFindsWorkBITSAccepted(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 64<<10)
	srv := staticServer(t, body)
	spec := specFor(t, srv.URL+"/payload.bin", digest, int64(len(body)))
	spec.Request = fmt.Sprintf("locate-test-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelAtEnd(t, d, id)

	// A delegate built from nothing, the way the process that has to recover
	// this handoff is: it never made the request and holds no state from it.
	found, err := New().Locate(ctx, spec.Request)
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if found != id {
		t.Fatalf("Locate = %q, BITS is running %q", found, id)
	}
}

// "I never took it" has to be a claim BITS actually made, and it must be
// distinguishable from BITS being unreachable — which is an error, not "".
func TestLocateIsCertainAboutWorkItNeverTook(t *testing.T) {
	d := withBITS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	found, err := d.Locate(ctx, fmt.Sprintf("never-submitted-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if found != "" {
		t.Fatalf("Locate invented a handle %q for work BITS never accepted", found)
	}
}

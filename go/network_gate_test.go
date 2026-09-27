package download

import (
	"context"
	"errors"
	"testing"

	"github.com/openabstractions/abstraction-download/go/netcost"
	job "github.com/openabstractions/abstraction-job/go"
)

func TestActiveUnmeteredGateStopsWhenCostSourceDisappears(t *testing.T) {
	source := netcost.NewFake(netcost.Unmetered)
	defer source.Close()
	runner := &Runner{Network: source}
	record := &job.Record{Requires: []string{string(CapNetworkCost)}}
	spec := Spec{Constraints: &Constraints{Network: NetworkUnmetered}}
	ctx, gate, err := runner.gateNetwork(context.Background(), record, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.stop()
	if err := networkAllows(ctx); err != nil {
		t.Fatalf("initial unmetered path: %v", err)
	}
	source.Set(netcost.CostUnavailable)
	if err := networkAllows(ctx); !errors.Is(err, ErrNetworkUnavailable) || waitingWord(err) != WaitingNetworkUnavailable {
		t.Fatalf("lost cost source: %v", err)
	}
	if !gate.held() || !gate.unavailable.Load() {
		t.Fatal("active transfer was not held on source loss")
	}
}

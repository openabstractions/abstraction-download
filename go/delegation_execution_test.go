package download

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type incapableExternal struct{ *fakeDelegate }

type claimedRecovery struct{ Delegator }

func (d claimedRecovery) Capabilities() []Capability {
	return append(d.Delegator.Capabilities(), CapRecoverableSubmission)
}

type recoverableExternal struct{ *acceptedThenLost }

func (d recoverableExternal) Capabilities() []Capability {
	return append(d.acceptedThenLost.Capabilities(), CapRecoverableSubmission)
}

func TestRecoverableSubmissionRefusesIncapableAdapters(t *testing.T) {
	for _, lying := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-capability", true: "claimed-without-locator"}[lying], func(t *testing.T) {
			body, digest := payload(t, 4096)
			r, store, fake, root := newDelegatingRunner(t, body)
			var adapter Delegator = fake
			if lying {
				adapter = claimedRecovery{fake}
			}
			r.Delegators = NewDelegators(adapter)
			id, err := Submit(store, Spec{Artifact: Artifact{Digest: digest, Size: int64(len(body))}, Sources: []Source{mirrored(t, root, body)}, Sink: Sink{Final: root + "/result"}}, string(CapRecoverableSubmission))
			if err != nil {
				t.Fatal(err)
			}
			if err = r.Delegate(context.Background(), id); !errors.Is(err, ErrNoDelegator) || !strings.Contains(err.Error(), string(CapRecoverableSubmission)) {
				t.Fatal(err)
			}
			if len(fake.jobs) != 0 {
				t.Fatal("incapable adapter caused external effect")
			}
			if n, err := r.Adopt(context.Background()); !errors.Is(err, ErrRecoverableSubmissionUnavailable) || n != 0 {
				t.Fatalf("weaker local adoption: %d %v", n, err)
			}
			rec, err := store.Load(id)
			if err != nil || rec.Delegation != nil || rec.State != "pending" {
				t.Fatal(rec, err)
			}
		})
	}
}

func TestRecoverableSubmissionDefiniteRefusalReleasesHandoff(t *testing.T) {
	body, digest := payload(t, 4096)
	r, store, lost, root := lostAfterAccepting(t, body)
	lost.fakeDelegate.startErr = errors.New("definitely refused before effect")
	r.Delegators = NewDelegators(recoverableExternal{lost})
	id, err := Submit(store, Spec{Artifact: Artifact{Digest: digest, Size: int64(len(body))}, Sources: []Source{mirrored(t, root, body)}, Sink: Sink{Final: root + "/result"}}, string(CapRecoverableSubmission))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Delegate(context.Background(), id); !errors.Is(err, ErrNoDelegator) {
		t.Fatal(err)
	}
	rec, err := store.Load(id)
	if err != nil || rec.Delegation != nil || len(lost.jobs) != 0 {
		t.Fatal(rec, err)
	}
}

func (*incapableExternal) Capabilities() []Capability { return []Capability{CapDelegates} }

func TestIncapableDelegateNamesRequiredCapabilityBeforeStart(t *testing.T) {
	body, digest := payload(t, 4096)
	r, store, fake, root := newDelegatingRunner(t, body)
	r.Delegators = NewDelegators(&incapableExternal{fake})
	id, err := Submit(store, Spec{Artifact: Artifact{Digest: digest, Size: int64(len(body))}, Sources: []Source{mirrored(t, root, body)}, Sink: Sink{Final: root + "/result"}}, string(CapSurvivesProcessExit))
	if err != nil {
		t.Fatal(err)
	}
	err = r.Delegate(context.Background(), id)
	if !errors.Is(err, ErrNoDelegator) || !strings.Contains(err.Error(), string(CapSurvivesProcessExit)) {
		t.Fatalf("required capability not identified: %v", err)
	}
	if len(fake.jobs) != 0 {
		t.Fatal("incapable adapter started external work")
	}
}

func TestRepeatedDelegateRetainsExistingExternalOwner(t *testing.T) {
	for _, unknown := range []bool{true, false} {
		t.Run(map[bool]string{true: "unknown", false: "accepted"}[unknown], func(t *testing.T) {
			body, digest := payload(t, 4096)
			r, store, original, root := lostAfterAccepting(t, body)
			r.Delegators = NewDelegators(recoverableExternal{original})
			if unknown {
				original.locate = func(string) (string, error) { return "", errors.New("reply unavailable") }
			}
			id, err := Submit(store, Spec{Artifact: Artifact{Digest: digest, Size: int64(len(body))}, Sources: []Source{mirrored(t, root, body)}, Sink: Sink{Final: root + "/result"}}, string(CapRecoverableSubmission))
			if err != nil {
				t.Fatal(err)
			}
			err = r.Delegate(context.Background(), id)
			if unknown && !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatal(err)
			}
			if !unknown && err != nil {
				t.Fatal(err)
			}
			<-original.accepted
			before, err := store.Load(id)
			if err != nil {
				t.Fatal(err)
			}
			// A replacement process sees a different available adapter. It must
			// retain the persisted external owner, including an uncertain handoff.
			_, _, replacement, _ := newDelegatingRunner(t, body)
			next := NewRunner(store, "replacement-worker")
			next.Delegators = NewDelegators(replacement)
			if err = next.Delegate(context.Background(), id); err == nil {
				t.Error("repeated delegation accepted another provider")
			}
			if len(replacement.jobs) != 0 {
				t.Error("replacement started external work")
			}
			after, err := store.Load(id)
			if err != nil {
				t.Fatal(err)
			}
			if after.Delegation == nil || *after.Delegation != *before.Delegation {
				t.Fatalf("handoff changed: before=%+v after=%+v", before.Delegation, after.Delegation)
			}
		})
	}
}

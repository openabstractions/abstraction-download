package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A delegate that delivers wrong bytes leaves no file it created at the
// destination, and a file the caller already had there stays [DL-R16].
func TestADelegatedMismatchRemovesOnlyTheFileTheDelegateCreated(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing bool
	}{
		{name: "delegate created the file", existing: false},
		{name: "caller already had a file", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, digest := payload(t, 16<<10)
			wrong, _ := payload(t, 16<<10)
			r, store, fd, root := newDelegatingRunner(t, wrong)
			id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
			final := finalOf(t, store, id)
			if tc.existing {
				if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(final, []byte("the caller's own file"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Delegate(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			fd.advance(fd.handleOf(t, store, id), int64(len(wrong)), DelegateTransferred)

			if err := r.Reconcile(context.Background(), id); !errors.Is(err, ErrDigestMismatch) {
				t.Fatalf("Reconcile = %v, want ErrDigestMismatch", err)
			}
			_, err := os.Stat(final)
			switch {
			case tc.existing && err != nil:
				t.Fatalf("the caller's file at the destination was removed: %v", err)
			case !tc.existing && err == nil:
				t.Fatal("the refused bytes the delegate wrote are still at the destination")
			case !tc.existing && !errors.Is(err, os.ErrNotExist):
				t.Fatal(err)
			}
		})
	}
}

// replacingDelegate finalises the way BITS does: it writes beside the
// destination and renames over whatever was there.
type replacingDelegate struct{ *fakeDelegate }

func (f replacingDelegate) Finalize(ctx context.Context, id, dest string) error {
	f.mu.Lock()
	j, ok := f.jobs[id]
	f.mu.Unlock()
	if !ok || j.state != DelegateTransferred {
		return errors.New("not transferred")
	}
	tmp := dest + ".delegate"
	if err := os.WriteFile(tmp, f.deliver, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dest)
}

// A delegate that replaces the caller's file with refused bytes leaves nothing
// of those bytes at the destination [DL-R16].
func TestADelegatedMismatchRemovesAFileTheDelegateReplaced(t *testing.T) {
	body, digest := payload(t, 16<<10)
	wrong, _ := payload(t, 16<<10)
	r, store, fd, root := newDelegatingRunner(t, wrong)
	r.Delegators = NewDelegators(replacingDelegate{fd})
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	final := finalOf(t, store, id)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("the caller's own file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	fd.advance(fd.handleOf(t, store, id), int64(len(wrong)), DelegateTransferred)

	if err := r.Reconcile(context.Background(), id); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Reconcile = %v, want ErrDigestMismatch", err)
	}
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused bytes that replaced the caller's file are still at the destination: %v", err)
	}
}

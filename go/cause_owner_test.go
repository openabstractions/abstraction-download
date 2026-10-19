package download

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"

	acceptance "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
)

func TestCauseOfKeepsFailureV2GoldenWords(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"credential", &CredentialError{Name: "fixture", Outcome: "unavailable"}, "credential"},
		{"digest", ErrDigestMismatch, "digest_mismatch"},
		{"oversize", ErrOversize, "oversize"},
		{"short", ErrShortTransfer, "short_transfer"},
		{"unauthorized", &StatusError{Code: http.StatusUnauthorized}, "unauthorized"},
		{"not found", &StatusError{Code: http.StatusNotFound}, "not_found"},
		{"refused", ErrRefused, "refused"},
		{"server", &StatusError{Code: http.StatusServiceUnavailable}, "server_error"},
		{"transport", &net.DNSError{Err: "inert timeout", IsTimeout: true}, "transport"},
		{"unknown", errors.New("an unclassified future failure"), "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, err := range []error{tc.err, fmt.Errorf("wrapped: %w", tc.err)} {
				cause := CauseOf(err)
				if cause != tc.want || cause == string(acceptance.FailureCauseResultLost) {
					t.Fatalf("cause=%q want=%q", cause, tc.want)
				}
				// failure@2 retains its published string field and exact document bytes.
				want := "{\n\"error\": \"fixture\",\n\"cause\": \"" + tc.want + "\"\n}\n"
				if got := string(EncodeFailure(&Failure{Error: "fixture", Cause: cause})); got != want {
					t.Fatalf("failure@2 bytes=%q want=%q", got, want)
				}
			}
		})
	}
	if got := CauseOf(nil); got != "" {
		t.Fatalf("nil cause=%q", got)
	}
	if got := string(EncodeFailure(&Failure{Error: "fixture"})); got != "{\n\"error\": \"fixture\"\n}\n" {
		t.Fatalf("failure@1 bytes changed: %q", got)
	}
}

func TestStatusCauseKeepsPublishedWords(t *testing.T) {
	for _, tc := range []struct {
		code    int
		refused bool
		want    string
	}{
		{http.StatusUnauthorized, true, "unauthorized"},
		{http.StatusForbidden, true, "unauthorized"},
		{http.StatusProxyAuthRequired, true, "unauthorized"},
		{http.StatusNotFound, true, "not_found"},
		{http.StatusGone, true, "not_found"},
		{http.StatusInternalServerError, true, "server_error"},
		{http.StatusBadRequest, true, "refused"},
		{http.StatusTooManyRequests, false, "other"},
		{0, false, "other"},
	} {
		status := &StatusError{Code: tc.code, refused: tc.refused}
		if got := status.Cause(); got != tc.want || CauseOf(status) != got {
			t.Fatalf("status=%d cause=%q want=%q", tc.code, got, tc.want)
		}
	}
}

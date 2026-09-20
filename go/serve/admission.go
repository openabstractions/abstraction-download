package serve

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
)

// admissionCheckBudget bounds one admission check of all of a request's
// credentials.
const admissionCheckBudget = 10 * time.Second

// outcomeWord is the shape of an applier outcome placed in a reason.
var outcomeWord = regexp.MustCompile(`^[a-z_]{1,32}$`)

// CheckAdmission refuses a submission naming a credential the submitting
// caller may not apply, before anything is journaled [JOB-A16, DL-K1]. For
// every source naming a credential it asks the applier's CredentialChecker
// with the submitting scope and the source host. A refusal returns invalid
// with credential:<outcome>:<name>; an applier that cannot answer returns
// unavailable with credential:unavailable:<name>. Requests that name no
// credential, a request preparation would refuse anyway, and an applier
// without a checker are admitted here.
func (e HTTPExecution) CheckAdmission(scope, kind string, raw []byte, required []string) (api.AcceptanceOutcome, string) {
	checker, ok := e.Credentials.(CredentialChecker)
	if !ok || kind != download.Kind || !slices.Contains(required, CredentialGuarantee) {
		return 0, ""
	}
	input, err := request.Decode(raw)
	if err != nil {
		return 0, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), admissionCheckBudget)
	defer cancel()
	checked := map[[2]string]bool{}
	for _, source := range input.Sources {
		if source.Credential == "" || !credentialName.MatchString(source.Credential) {
			continue
		}
		u, err := url.Parse(source.Locator)
		if err != nil || u.Hostname() == "" {
			continue
		}
		use := [2]string{source.Credential, strings.ToLower(u.Hostname())}
		if checked[use] {
			continue
		}
		checked[use] = true
		err = checker.CheckCredential(ctx, scope, use[0], use[1])
		if err == nil {
			continue
		}
		var refusal *download.CredentialError
		if !errors.As(err, &refusal) || refusal.Retryable() {
			return api.AcceptanceOutcomeUnavailable, "credential:unavailable:" + use[0]
		}
		if !outcomeWord.MatchString(refusal.Outcome) {
			return api.AcceptanceOutcomeInvalid, "credential:refused:" + use[0]
		}
		return api.AcceptanceOutcomeInvalid, "credential:" + refusal.Outcome + ":" + use[0]
	}
	return 0, ""
}

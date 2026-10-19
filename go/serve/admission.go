package serve

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// admissionCheckBudget bounds one admission check of all of a request's
// credentials.
const admissionCheckBudget = 10 * time.Second

// outcomeWord is the shape of an applier outcome placed in a reason.
var outcomeWord = regexp.MustCompile(`^[a-z_]{1,32}$`)

// credentialNotPermitted is the applier's outcome for a rights decision that
// refused the caller the credential (credentials CRED-A2).
const credentialNotPermitted = "not_permitted"

// CheckAdmission refuses a submission whose every source host the person
// switched off, unavailable with the list's reason [DL-E8], and a submission
// naming a credential the submitting caller may not apply, before anything is
// journaled [JOB-A16, DL-K1]. For
// every source naming a credential it asks the applier's CredentialChecker
// with the submitting scope and the source host. A rights refusal
// (not_permitted) returns forbidden and any other refusal invalid, each with
// credential:<outcome>:<name>; an applier that cannot answer returns
// unavailable with credential:unavailable:<name>. Requests that name no
// credential, a request preparation would refuse anyway, and an applier
// without an original checker are admitted here on credentials@1. The scoped
// route requires its checker and the receiving boundary's subject.
func (e HTTPExecution) CheckAdmission(scope, kind string, raw []byte, required []string) (api.AcceptanceOutcome, string) {
	if outcome, reason := schemeRefusal(kind, raw); outcome != 0 {
		return outcome, reason
	}
	if outcome, reason := refusedHosts(kind, raw); outcome != 0 {
		return outcome, reason
	}
	if kind == download.Kind {
		route, err := credentialRouteFor(required)
		if err != nil {
			return api.AcceptanceOutcomeInvalid, err.Error()
		}
		if route == ScopedCredentialGuarantee {
			return api.AcceptanceOutcomeUnavailable, "credential:unavailable:subject"
		}
	}
	checker, ok := e.Credentials.(CredentialChecker)
	if !ok {
		return 0, ""
	}
	return checkCredentialsAdmission(kind, raw, required, func(ctx context.Context, name, host string) error {
		return checker.CheckCredential(ctx, scope, name, host)
	})
}

func (e HTTPExecution) CheckSubjectAdmission(binding acceptanceprovider.Binding, kind string, raw []byte, required []string) (api.AcceptanceOutcome, string) {
	if outcome, reason := schemeRefusal(kind, raw); outcome != 0 {
		return outcome, reason
	}
	if outcome, reason := refusedHosts(kind, raw); outcome != 0 {
		return outcome, reason
	}
	if kind == download.Kind {
		route, err := credentialRouteFor(required)
		if err != nil {
			return api.AcceptanceOutcomeInvalid, err.Error()
		}
		if route == ScopedCredentialGuarantee {
			if binding.Subject == nil || !scopedCredentialsAvailable(e.Credentials) {
				return api.AcceptanceOutcomeUnavailable, "credential:unavailable:subject"
			}
			checker := e.Credentials.(ScopedSubjectCredentialChecker)
			return checkCredentialsAdmission(kind, raw, required, func(ctx context.Context, name, host string) error {
				return checker.CheckScopedSubjectCredential(ctx, *binding.Subject, name, host)
			})
		}
	}
	checker, ok := e.Credentials.(SubjectCredentialChecker)
	if !ok || binding.Subject == nil {
		if kind == download.Kind && requiresCredentials(required) {
			return api.AcceptanceOutcomeUnavailable, "credential:unavailable:subject"
		}
		return 0, ""
	}
	return checkCredentialsAdmission(kind, raw, required, func(ctx context.Context, name, host string) error {
		return checker.CheckSubjectCredential(ctx, *binding.Subject, name, host)
	})
}

func checkCredentialsAdmission(kind string, raw []byte, required []string, check func(context.Context, string, string) error) (api.AcceptanceOutcome, string) {
	if kind != download.Kind || !requiresCredentials(required) {
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
		if !download.CredentialTransport(u) {
			// A credential sent in clear is refused before the person is asked
			// about it [DL-K5].
			return api.AcceptanceOutcomeInvalid, "credential:" + download.OutcomeInsecureTransport + ":" + source.Credential
		}
		use := [2]string{source.Credential, strings.ToLower(u.Hostname())}
		if checked[use] {
			continue
		}
		checked[use] = true
		err = check(ctx, use[0], use[1])
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
		if refusal.Outcome == credentialNotPermitted {
			// The rights decision refused this caller the credential: a
			// rights refusal, unsealed like any admission refusal [JOB-A16].
			// It names each first-use question the check raised or joined
			// that is still pending, so the submitter's client waits for the
			// person's answer and submits again (INF-W11).
			return api.AcceptanceOutcomeForbidden, namePendingQuestions("credential:"+refusal.Outcome+":"+use[0], err)
		}
		return api.AcceptanceOutcomeInvalid, "credential:" + refusal.Outcome + ":" + use[0]
	}
	return 0, ""
}

// questionPendingPrefix begins the refusal word naming one pending first-use
// question (inference INF-W11).
const questionPendingPrefix = "rights:question_pending:"

// maxAdmissionReason is the acceptance reason bound (JOB-A16).
const maxAdmissionReason = 1024

// namePendingQuestions appends " rights:question_pending:<id>" to reason for
// each question a checker's refusal carries through an optional
// PendingQuestions method, in order and once each. The grant words come first
// and stay whole; naming stops at the acceptance reason bound.
func namePendingQuestions(reason string, err error) string {
	var carrier interface{ PendingQuestions() []string }
	if !errors.As(err, &carrier) {
		return reason
	}
	seen := map[string]bool{}
	for _, id := range carrier.PendingQuestions() {
		if id == "" || strings.ContainsAny(id, " \t\r\n") || seen[id] {
			continue
		}
		word := " " + questionPendingPrefix + id
		if len(reason)+len(word) > maxAdmissionReason {
			break
		}
		seen[id] = true
		reason += word
	}
	return reason
}

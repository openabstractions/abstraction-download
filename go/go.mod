module github.com/ReinisLusis/abstraction-download

go 1.26.0

require github.com/ReinisLusis/abstraction-job v0.0.0

require (
	github.com/ReinisLusis/abstraction-config v0.0.0
	golang.org/x/crypto/x509roots/fallback v0.0.0-20260902174831-a6cdac608407
)

replace github.com/ReinisLusis/abstraction-job => ../../job/go

replace github.com/ReinisLusis/abstraction-config => ../../config/go

require github.com/ReinisLusis/abstraction-storage v0.0.0

replace github.com/ReinisLusis/abstraction-storage => ../../storage/go

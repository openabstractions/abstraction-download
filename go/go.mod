module github.com/ReinisLusis/abstraction/download/go

go 1.26.0

require github.com/ReinisLusis/abstraction/job/go v0.0.0

require (
	github.com/ReinisLusis/abstraction/config/go v0.0.0
	golang.org/x/crypto/x509roots/fallback v0.0.0-20260902174831-a6cdac608407
)

replace github.com/ReinisLusis/abstraction/job/go => ../../job/go

replace github.com/ReinisLusis/abstraction/config/go => ../../config/go

require github.com/ReinisLusis/abstraction/storage/go v0.0.0

replace github.com/ReinisLusis/abstraction/storage/go => ../../storage/go

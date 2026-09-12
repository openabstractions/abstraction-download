# Installed runtime parent

This provider-support package launches the exact sibling `openabstractions.exe`
as `serve runtime --supervised`. An explicit executable override must be absolute.
Args append runtime flags and cannot override supervision. The package imports
platform APIs and Go standard libraries; central runtime/facade code stays in the
child. Other platforms explicitly refuse until equivalent containment exists.

`Start(ctx, Options)` waits for `READY 1\n` with StartupTimeout. The parent owns
the sole stdin writer. `Close` closes it, waits ShutdownTimeout, then requests
job termination with ForceTimeout. Context cancellation initiates Close.
`Wait` returns the observed child failure and tree-cleanup failure. ErrForced is
returned for forced shutdown; callers can inspect joined underlying errors.
The defaults are 10s startup, 5s graceful shutdown, 5s forced completion. A nil
Stderr uses an owned NUL file, supporting a GUI/service parent with no console;
an explicitly supplied diagnostic file must be valid. The Windows console is
hidden with CREATE_NO_WINDOW.

Windows 10/Server 2016 or newer is required for PROC_THREAD_ATTRIBUTE_JOB_LIST.
CreateProcess receives the unnamed KILL_ON_JOB_CLOSE job atomically, plus an
explicit standard-handle inheritance list. Neither the job handle nor the pipe
writer is inherited. Unsupported attributes/nested-job restrictions fail creation.
Parent death closes the last job handle and Windows terminates its process tree.
No suspended-child or create-then-assign fallback exists.

While the parent lives, primary exit triggers termination of remaining job
members. Before termination the package captures waitable process handles;
it then observes zero active job processes and the captured process signals
within one force budget. A changed total-process count during capture/termination
fails with incomplete-observation evidence. Arbitrary descendant churn is not
reported as verified complete. OS calls themselves remain subject to Windows
scheduling; the software budgets bound observation loops and waits.

This establishes process containment. Accepted work still needs application-level
persistence/fencing when forced termination occurs. It registers no OS service.

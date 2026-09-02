package nas

import (
	config "github.com/ReinisLusis/abstraction-config"
	download "github.com/ReinisLusis/abstraction-download"
)

// Registering here is what makes a NAS available to an application that has
// never heard of one. Blank-import this package (or .../download/all) and
// download.Discover picks it up.
func init() {
	download.RegisterTier(download.Tier{
		Name: System,
		// First. A NAS is always on and the machine asking usually is not, which
		// is the entire reason to prefer it.
		Priority: 10,
		New: func(cfg config.Config) (download.Delegator, error) {
			d, err := New(cfg.NASStore)
			if err != nil {
				return nil, err
			}
			// Probe, do not assume. A NAS that is switched off must not be
			// registered: a job written into a directory nobody is watching looks
			// exactly like a download that started, and would sit there forever.
			if err := d.Available(); err != nil {
				return nil, err
			}
			return d, nil
		},
	})
}

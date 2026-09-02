package bits

import (
	config "github.com/ReinisLusis/abstraction-config"
	download "github.com/ReinisLusis/abstraction-download"
)

// Registering here is what makes the OS transfer service available without any
// application naming it. Nothing is configured: BITS is either on this machine
// or it is not, which is the closest thing to true zero-configuration discovery
// in the whole chain.
func init() {
	download.RegisterTier(download.Tier{
		Name: System,
		// After the NAS. BITS survives this process and a reboot, but not the
		// machine being asleep.
		Priority: 20,
		New: func(config.Config) (download.Delegator, error) {
			d := New()
			if err := d.Available(); err != nil {
				return nil, err
			}
			return d, nil
		},
	})
}

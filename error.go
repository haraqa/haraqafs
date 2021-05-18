package haraqafs

import "fmt"

func aggErrors(errs []error) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return fmt.Errorf("multiple errors received %+v", errs)
	}
}

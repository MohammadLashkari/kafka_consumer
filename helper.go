package main

import (
	"errors"
	"fmt"
	"time"
)

func Retry(
	attempts int, delay time.Duration, onRetry func(attempt int, err error), fn func() error,
) error {
	var allErrs []error
	for i := range attempts {
		err := fn()
		if err == nil {
			return nil
		}
		allErrs = append(allErrs, fmt.Errorf("attempt #%d: %w", i+1, err))

		if i < attempts-1 {
			if onRetry != nil {
				onRetry(i+1, err)
			}
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}

	return errors.Join(allErrs...)
}

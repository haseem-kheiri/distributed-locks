package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// =========================
// Models
// =========================

type LockLease struct {
	Name        string
	ExecutionID string
	ExpiresAt   time.Time
}

type LockExecutionResult[T any] struct {
	Invoked     bool
	ExecutionID string
	Result      T
}

// Internal tracker to link a lease with its specific context cancellation
// NOTE: lease pointer is kept stable; we only mutate fields (e.g., ExpiresAt)
// to avoid pointer replacement races with user code.
type activeLease struct {
	lease          *LockLease
	cancel         context.CancelFunc
	lastSuccessful time.Time
}

// =========================
// Provider Interface
// =========================

type LockProvider interface {
	AcquireLock(ctx context.Context, lockName string, executionID string, leaseDuration time.Duration) (*LockLease, error)
	Release(ctx context.Context, leases []*LockLease) error
	Renew(ctx context.Context, leases []*LockLease, leaseDuration time.Duration) ([]*LockLease, error)
}

// =========================
// Manager
// =========================

type lockManager struct {
	lockProvider LockProvider
	mu           sync.RWMutex
	activeLocks  map[string]activeLease
}

func NewLockManager(ctx context.Context, provider LockProvider) *lockManager {
	m := &lockManager{
		lockProvider: provider,
		activeLocks:  make(map[string]activeLease),
	}
	m.start(ctx)
	return m
}

// =========================
// Config
// =========================

const (
	RenewInterval     = 3 * time.Second
	RenewalThreshold  = 9 * time.Second
	LockLeaseDuration = 30 * time.Second
)

// =========================
// Public API
// =========================

func TryLock[T any](
	ctx context.Context,
	m *lockManager,
	lockName string,
	block func(ctx context.Context, lease *LockLease) (T, error),
) (LockExecutionResult[T], error) {

	executionID := uuid.New().String()

	lock, err := m.lockProvider.AcquireLock(ctx, lockName, executionID, LockLeaseDuration)
	if err != nil {
		return LockExecutionResult[T]{ExecutionID: executionID}, err
	}
	if lock == nil {
		return LockExecutionResult[T]{ExecutionID: executionID, Invoked: false}, nil
	}

	// leaseCtx is cancelled if the renewal loop fails OR if TryLock finishes
	leaseCtx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	m.activeLocks[lock.ExecutionID] = activeLease{
		lease:          lock,
		cancel:         cancel,
		lastSuccessful: time.Now(),
	}
	m.mu.Unlock()

	defer func() {
		// 1. Stop the context/renewal signaling
		cancel()

		// 2. Clean up local state
		m.mu.Lock()
		delete(m.activeLocks, lock.ExecutionID)
		m.mu.Unlock()

		// 3. Inform the provider (bounded context)
		releaseCtx, rCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rCancel()
		if err := m.lockProvider.Release(releaseCtx, []*LockLease{lock}); err != nil {
			fmt.Printf("Error releasing lock: %v\n", err)
		}
	}()

	var result T
	var blockErr error

	// panic-safe execution
	func() {
		defer func() {
			if r := recover(); r != nil {
				blockErr = fmt.Errorf("panic: %v", r)
			}
		}()
		result, blockErr = block(leaseCtx, lock)
	}()

	return LockExecutionResult[T]{
		Invoked:     true,
		ExecutionID: executionID,
		Result:      result,
	}, blockErr
}

// =========================
// Renewal Loop
// =========================

func (m *lockManager) renewLeases(ctx context.Context) {
	var toRenew []*LockLease
	m.mu.RLock()
	for _, al := range m.activeLocks {
		if time.Until(al.lease.ExpiresAt) <= RenewalThreshold {
			toRenew = append(toRenew, al.lease)
		}
	}
	m.mu.RUnlock()

	// 1. Bulk Renew
	renewedMap := make(map[string]*LockLease)
	if len(toRenew) > 0 {
		if renewed, err := m.lockProvider.Renew(ctx, toRenew, LockLeaseDuration); err == nil {
			for _, r := range renewed {
				renewedMap[r.ExecutionID] = r
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()

	// 2. Comprehensive State Check
	for id, al := range m.activeLocks {
		// Update if renewal succeeded
		if r, ok := renewedMap[id]; ok {
			al.lease.ExpiresAt = r.ExpiresAt
			al.lastSuccessful = now
			m.activeLocks[id] = al
			continue
		}

		// Hard Safety Cutoff: Stop if we haven't confirmed ownership recently.
		// Use a buffer to beat the server-side TTL.
		safetyBuffer := RenewInterval * 2

		if now.Sub(al.lastSuccessful) >= (LockLeaseDuration - safetyBuffer) {
			al.cancel()
			delete(m.activeLocks, id)
		}
	}
}

func (m *lockManager) start(ctx context.Context) {
	ticker := time.NewTicker(RenewInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.renewLeases(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Package kubernetes elects through a coordination.k8s.io/v1 Lease, with client-go's
// leaderelection. The replica needs RBAC to get, create and update leases in its namespace.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const serviceAccountNamespace = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

type Config struct {
	Client    k8s.Interface
	Namespace string
	LeaseName string
	Identity  string
	// TTL is the lease duration. The leader stops when it could not renew within TTL - Margin
	// (client-go's RenewDeadline) and retries every RenewEvery.
	TTL        time.Duration
	RenewEvery time.Duration
	Margin     time.Duration
	// Pool is shared by the electors of the process to spread jobs (see leader.Lease.Run).
	Pool *leader.Pool
	// Yield, when set, lets jobs yield or release their lease (see leader.MaybeYield).
	Yield *leader.YieldConfig
	Logf  func(format string, args ...any)
}

// Electors gives each job its own Lease, named Prefix + "-" + job.
type Electors struct {
	Base   Config // LeaseName unused; Pool shared by every job
	Prefix string
}

var _ leader.Electors = (*Electors)(nil)

// For returns the elector of job. Config errors were reported by NewElectors.
func (e *Electors) For(job string) leader.Elector {
	cfg := e.Base
	cfg.LeaseName = e.Prefix + "-" + job
	el, err := New(cfg)
	if err != nil {
		return failed{err: err, logf: cfg.Logf}
	}
	return el
}

// NewElectors validates base (with a sample lease name) and returns per-job electors.
func NewElectors(base Config, prefix string) (*Electors, error) {
	if prefix == "" {
		return nil, errors.New("kubernetes electors need a lease name prefix")
	}
	if base.Pool == nil {
		base.Pool = &leader.Pool{}
	}
	if base.Logf == nil {
		base.Logf = log.Printf
	}
	probe := base
	probe.LeaseName = prefix + "-probe"
	if _, err := New(probe); err != nil {
		return nil, err
	}
	return &Electors{Base: base, Prefix: prefix}, nil
}

// failed is an elector that never leads, for a config error found late.
type failed struct {
	err  error
	logf func(format string, args ...any)
}

func (f failed) Run(ctx context.Context, _ func(ctx context.Context)) {
	f.logf("[Leader] Kubernetes elector unusable: %v", f.err)
	<-ctx.Done()
}

// Elector is a leader.Elector on a Kubernetes Lease.
type Elector struct {
	cfg     Config
	lock    resourcelock.Interface
	handoff *leader.Handoff
}

var _ leader.Elector = (*Elector)(nil)

func New(cfg Config) (*Elector, error) {
	if cfg.Client == nil {
		return nil, errors.New("kubernetes elector needs a client")
	}
	if cfg.Namespace == "" || cfg.LeaseName == "" || cfg.Identity == "" {
		return nil, errors.New("kubernetes elector needs a namespace, a lease name and an identity")
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	// client-go requires LeaseDuration > RenewDeadline > RetryPeriod * 1.2.
	if renew := cfg.TTL - cfg.Margin; cfg.Margin <= 0 || float64(renew) <= float64(cfg.RenewEvery)*leaderelection.JitterFactor {
		return nil, fmt.Errorf("lease TTL - margin (%s) must exceed %.1f x the renew interval (%s)",
			renew, leaderelection.JitterFactor, cfg.RenewEvery)
	}
	if cfg.Yield != nil {
		if err := cfg.Yield.Validate(); err != nil {
			return nil, err
		}
	}
	return &Elector{cfg: cfg, handoff: leader.NewHandoff(cfg.Yield), lock: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: cfg.LeaseName, Namespace: cfg.Namespace},
		Client:     cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: cfg.Identity},
	}}, nil
}

// NewFromEnvironment builds the client from the in-cluster service account, or from KUBECONFIG
// outside a cluster, defaults the namespace to the service account's, and returns per-job
// electors with lease names prefix + "-" + job.
func NewFromEnvironment(cfg Config, prefix string) (*Electors, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		path := os.Getenv("KUBECONFIG")
		if path == "" {
			return nil, fmt.Errorf("not in a cluster and KUBECONFIG is not set: %w", err)
		}
		if restCfg, err = clientcmd.BuildConfigFromFlags("", path); err != nil {
			return nil, err
		}
	}
	if cfg.Client, err = k8s.NewForConfig(restCfg); err != nil {
		return nil, err
	}
	if cfg.Namespace == "" {
		b, err := os.ReadFile(serviceAccountNamespace)
		if err != nil {
			return nil, fmt.Errorf("LEADER_K8S_NAMESPACE not set and no service account namespace: %w", err)
		}
		cfg.Namespace = strings.TrimSpace(string(b))
	}
	return NewElectors(cfg, prefix)
}

// Run leads while client-go holds the Lease. The lease is only released after fn has returned,
// so the next leader never starts while this one's work is still running. Rounds are spaced like
// leader.Lease attempts (jitter, plus RenewEvery per lease the process already holds), and a round
// that sees another holder stops at once, so less-loaded replicas take free leases first.
func (e *Elector) Run(ctx context.Context, fn func(ctx context.Context)) {
	wait := jitter(e.cfg.RenewEvery)
	for ctx.Err() == nil {
		if !sleep(ctx, wait) {
			return
		}
		if held := e.cfg.Pool.Held(); held > 0 && !sleep(ctx, e.cfg.RenewEvery*time.Duration(held)) {
			return
		}
		if term := e.runOnce(ctx, fn); term != nil {
			wait = e.handoff.End(term) // cooldown after a yield or release, else 0
			if wait > 0 {
				e.cfg.Logf("[Leader] %s waits %s before asking for %s/%s again", e.cfg.Identity, wait, e.cfg.Namespace, e.cfg.LeaseName)
			}
		} else {
			wait = e.cfg.RenewEvery + jitter(e.cfg.RenewEvery)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// runOnce runs one round and returns the term when this process led in it, else nil.
func (e *Elector) runOnce(ctx context.Context, fn func(ctx context.Context)) *leader.Term {
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	var mu sync.Mutex
	var fnDone chan struct{}
	var term *leader.Term

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            e.lock,
		Name:            e.cfg.LeaseName,
		LeaseDuration:   e.cfg.TTL,
		RenewDeadline:   e.cfg.TTL - e.cfg.Margin,
		RetryPeriod:     e.cfg.RenewEvery,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				e.cfg.Pool.Add(1)
				defer e.cfg.Pool.Add(-1)
				done := make(chan struct{})
				mu.Lock()
				fnDone = done
				mu.Unlock()
				defer close(done)
				e.handoff.Observed(false)
				termCtx, t := e.handoff.Begin(leadCtx, e.cfg.Pool)
				mu.Lock()
				term = t
				mu.Unlock()
				workCtx, cancel := context.WithCancel(termCtx)
				stop := context.AfterFunc(ctx, cancel)
				e.cfg.Logf("[Leader] %s acquired Kubernetes lease %s/%s", e.cfg.Identity, e.cfg.Namespace, e.cfg.LeaseName)
				fn(workCtx)
				stop()
				cancel()
				if outcome := t.Outcome(); outcome != "" {
					e.cfg.Logf("[Leader] %s %s %s/%s", e.cfg.Identity, outcome, e.cfg.Namespace, e.cfg.LeaseName)
				}
				// Work is over (shutdown, lost lease, or it returned): release the lease now.
				stopRun()
			},
			OnNewLeader: func(identity string) {
				mu.Lock()
				leading := fnDone != nil
				mu.Unlock()
				if identity != e.cfg.Identity && !leading {
					e.handoff.Observed(true)
					stopRun() // held elsewhere: back off instead of polling every retry period
				}
			},
			OnStoppedLeading: func() {
				e.cfg.Logf("[Leader] %s stopped leading Kubernetes lease %s/%s", e.cfg.Identity, e.cfg.Namespace, e.cfg.LeaseName)
			},
		},
	})
	if err != nil {
		e.cfg.Logf("[Leader] Invalid Kubernetes leader election config: %v", err)
		return nil
	}

	// Shutting down before or without leading: stop campaigning.
	stopWatch := context.AfterFunc(ctx, func() {
		mu.Lock()
		done := fnDone
		mu.Unlock()
		if done == nil {
			stopRun()
		}
	})
	defer stopWatch()

	le.Run(runCtx)
	mu.Lock()
	done := fnDone
	mu.Unlock()
	if done != nil {
		<-done
	}
	mu.Lock()
	defer mu.Unlock()
	return term
}

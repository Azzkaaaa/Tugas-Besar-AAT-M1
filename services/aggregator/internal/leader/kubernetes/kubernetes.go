// Package kubernetes elects through a coordination.k8s.io/v1 Lease, with client-go's
// leaderelection. The replica needs RBAC to get, create and update leases in its namespace.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	Logf       func(format string, args ...any)
}

// Elector is a leader.Elector on a Kubernetes Lease.
type Elector struct {
	cfg  Config
	lock resourcelock.Interface
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
	return &Elector{cfg: cfg, lock: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: cfg.LeaseName, Namespace: cfg.Namespace},
		Client:     cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: cfg.Identity},
	}}, nil
}

// NewFromEnvironment builds the client from the in-cluster service account, or from KUBECONFIG
// outside a cluster, and defaults the namespace to the service account's.
func NewFromEnvironment(cfg Config) (*Elector, error) {
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
	return New(cfg)
}

// Run leads while client-go holds the Lease. The lease is only released after fn has returned,
// so the next leader never starts while this one's work is still running.
func (e *Elector) Run(ctx context.Context, fn func(ctx context.Context)) {
	for ctx.Err() == nil {
		e.runOnce(ctx, fn)
		select {
		case <-ctx.Done():
		case <-time.After(e.cfg.RenewEvery):
		}
	}
}

func (e *Elector) runOnce(ctx context.Context, fn func(ctx context.Context)) {
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	var mu sync.Mutex
	var fnDone chan struct{}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            e.lock,
		Name:            e.cfg.LeaseName,
		LeaseDuration:   e.cfg.TTL,
		RenewDeadline:   e.cfg.TTL - e.cfg.Margin,
		RetryPeriod:     e.cfg.RenewEvery,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				done := make(chan struct{})
				mu.Lock()
				fnDone = done
				mu.Unlock()
				defer close(done)
				workCtx, cancel := context.WithCancel(leadCtx)
				stop := context.AfterFunc(ctx, cancel)
				e.cfg.Logf("[Leader] %s acquired Kubernetes lease %s/%s", e.cfg.Identity, e.cfg.Namespace, e.cfg.LeaseName)
				fn(workCtx)
				stop()
				cancel()
				// Work is over (shutdown, lost lease, or it returned): release the lease now.
				stopRun()
			},
			OnStoppedLeading: func() {
				e.cfg.Logf("[Leader] %s stopped leading Kubernetes lease %s/%s", e.cfg.Identity, e.cfg.Namespace, e.cfg.LeaseName)
			},
		},
	})
	if err != nil {
		e.cfg.Logf("[Leader] Invalid Kubernetes leader election config: %v", err)
		return
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
}

package pkg

import (
	"sync/atomic"
	"time"

	nebula_ng "github.com/vesoft-inc/nebula-go/v5"
	"github.com/vesoft-inc/nebula-go/v5/pkg/types"
	"golang.org/x/sync/errgroup"
)

type ScanType int

const (
	ScanTypeDefault ScanType = iota
	ScanTypeNoScan
	ScanTypeNoReuse
)

type Controller struct {
	config   *config
	scanType ScanType
}

type Output struct {
	latencyUs  int64 `json:"latency_us"`
	responseUs int64 `json:"response_us"`
}

func NewController(cfg *config, typ ScanType) *Controller {
	return &Controller{
		config:   cfg,
		scanType: typ,
	}
}

func (c *Controller) Run() (int64, int64, error) {
	pool, err := nebula_ng.NewNebulaPool(
		c.config.address,
		c.config.user,
		c.config.password,
	)
	if err != nil {
		return 0, 0, err
	}
	defer pool.Close()
	var errGroup errgroup.Group
	var totalLatencyUs atomic.Int64
	var totalResponseUs atomic.Int64
	for i := 0; i < c.config.concurrency; i++ {
		client, err := pool.GetClient()
		if err != nil {
			return 0, 0, err
		}
		defer client.Close()
		errGroup.Go(
			func() error {
				output, err := c.runWorker(client)
				if err != nil {
					return err
				}
				totalLatencyUs.Add(output.latencyUs)
				totalResponseUs.Add(output.responseUs)
				return nil
			})
	}
	if err := errGroup.Wait(); err != nil {
		return 0, 0, err
	}
	avgLatencyUs := totalLatencyUs.Load() / int64(c.config.concurrency*c.config.iterationsPerConcurrency)
	avgResponseUs := totalResponseUs.Load() / int64(c.config.concurrency*c.config.iterationsPerConcurrency)
	return avgLatencyUs, avgResponseUs, nil

}

func (c *Controller) runWorker(client types.Client) (*Output, error) {
	var totalLatencyUs int64
	var totalResponseUs int64
	for i := 0; i < c.config.iterationsPerConcurrency; i++ {
		start := time.Now()
		r, err := client.Execute(c.config.statement)
		if err != nil {
			return nil, err
		}
		totalLatencyUs += r.Summary().TotalServerTimeUs()
		switch c.scanType {
		case ScanTypeNoScan:
			// do nothing
		case ScanTypeNoReuse:
			for r.HasNext() {
				_, err := r.Next()
				if err != nil {
					return nil, err
				}
			}
		case ScanTypeDefault:
			values := make([]nebula_ng.NullValue, len(r.Columns()))
			anyValues := make([]any, len(r.Columns()))
			for i := range values {
				anyValues[i] = &values[i]
			}
			for r.HasNext() {
				if err := r.Scan(anyValues...); err != nil {
					return nil, err
				}
			}
		}
		totalResponseUs += time.Since(start).Microseconds()
	}
	return &Output{
		latencyUs:  totalLatencyUs,
		responseUs: totalResponseUs,
	}, nil
}

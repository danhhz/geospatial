package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func throughputRead(conn, file string, cfg *s2IndexConfig, concurrency int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return errors.Wrapf(err, "connecting to: %s", conn)
	}

	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)

	queryOp := containsOperation
	qr, err := makeQueryReader(file, querySelectivity, queryOp)
	if err != nil {
		return err
	}
	queries := make([]query, 1000)
	for i := 0; i < len(queries); {
		if err := ctx.Err(); err != nil {
			return nil
		}
		q, ok := qr.Next()
		if !ok {
			panic(`WIP`)
		}
		queries[i] = q
		i++
	}

	queryCh := make(chan query, 100)
	g.Go(func() error {
		for i := 0; ; i++ {
			if i >= len(queries) {
				i = 0
			}
			select {
			case queryCh <- queries[i]:
			case <-ctx.Done():
				return nil
			}
		}
	})

	const level = 10
	const histSigFigs = 1
	start := time.Now()
	resultsMu := struct {
		sync.Mutex
		queries               int
		lastUpdate            time.Time
		nanosHist, countsHist *hdrhistogram.Histogram
	}{
		lastUpdate: start,
		nanosHist: hdrhistogram.New(
			histMinLatency.Nanoseconds(), histMaxLatency.Nanoseconds(), histSigFigs),
		countsHist: hdrhistogram.New(
			histMinLatency.Nanoseconds(), histMaxLatency.Nanoseconds(), histSigFigs),
	}

	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				qGetStart := time.Now()
				var q query
				select {
				case <-ctx.Done():
					return nil
				case q = <-queryCh:
				}
				qGetDone := time.Since(qGetStart)
				_ = qGetDone

				qStart := time.Now()
				var count int64
				var err error
				if cfg != nil {
					count, err = q.ReadS2(db, cfg, level)
				} else {
					count, err = q.ReadPostGIS(db, level)
				}
				qDuration := time.Since(qStart)
				if err == errQuerySkipped {
					continue
				} else if err != nil {
					return err
				}
				_ = qDuration

				{
					now := time.Now()
					running := now.Sub(start)
					resultsMu.Lock()
					resultsMu.queries++
					resultsMu.nanosHist.RecordValue(qDuration.Nanoseconds())
					resultsMu.countsHist.RecordValue(count)
					if now.Sub(resultsMu.lastUpdate) > updateInterval {
						resultsMu.lastUpdate = now
						fmt.Printf("finished %d queries in %s\n", resultsMu.queries, running)
					}
					resultsMu.Unlock()
					if running > throughputMaxDuration {
						cancel()
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	fmt.Printf("finished query type %s shape %s concurrency %d\n", queryOp, queryShape, concurrency)
	fmt.Println("concurrency__meters_____numQ_pMin(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__count50__count95__count99_countMax")
	resultsMu.Lock()
	defer resultsMu.Unlock()
	nanosHist, countsHist := resultsMu.nanosHist, resultsMu.countsHist
	if nanosHist.TotalCount() == 0 {
		return nil
	}
	fmt.Printf("%11d %7d %8d %8.2f %8.2f %8.2f %8.2f %8.2f %8d %8d %8d %8d\n",
		concurrency,
		int(metersFromLevel(level)),
		nanosHist.TotalCount(),
		time.Duration(nanosHist.Min()).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(nanosHist.Max()).Seconds()*1000,
		countsHist.ValueAtQuantile(50),
		countsHist.ValueAtQuantile(95),
		countsHist.ValueAtQuantile(99),
		countsHist.Max(),
	)

	return nil
}

func throughputWrite(conn, file string, cfg *s2IndexConfig, concurrency int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rr, err := makeRoadReader(file)
	if err != nil {
		return err
	}
	defer rr.Close()

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return errors.Wrapf(err, "connecting to: %s", conn)
	}

	var idAtomic int64
	if err := db.QueryRow(`SELECT max(id) FROM roads`).Scan(&idAtomic); err != nil {
		return errors.Wrapf(err, "determining max id")
	}

	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)
	queries := make([]road, 1000)
	for i := 0; i < len(queries); {
		if err := ctx.Err(); err != nil {
			return nil
		}
		road, ok := rr.Next()
		if !ok {
			panic(`WIP`)
		}
		road.lls = append([]s2.LatLng(nil), road.lls...)
		road.geometry = string(append([]byte(nil), road.geometry...))
		queries[i] = road
		i++
	}

	queryCh := make(chan road, 100)
	g.Go(func() error {
		for i := 0; ; i++ {
			if i >= len(queries) {
				i = 0
			}
			select {
			case queryCh <- queries[i]:
			case <-ctx.Done():
				return nil
			}
		}
	})

	const level = 10
	const histSigFigs = 1
	start := time.Now()
	resultsMu := struct {
		sync.Mutex
		queries               int
		lastUpdate            time.Time
		nanosHist, countsHist *hdrhistogram.Histogram
	}{
		lastUpdate: start,
		nanosHist: hdrhistogram.New(
			histMinLatency.Nanoseconds(), histMaxLatency.Nanoseconds(), histSigFigs),
		countsHist: hdrhistogram.New(
			histMinLatency.Nanoseconds(), histMaxLatency.Nanoseconds(), histSigFigs),
	}

	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				qGetStart := time.Now()
				var road road
				select {
				case <-ctx.Done():
					return nil
				case road = <-queryCh:
				}
				qGetDone := time.Since(qGetStart)
				_ = qGetDone

				resultsMu.Lock()
				resultsMu.queries++
				resultsMu.Unlock()
				id := atomic.AddInt64(&idAtomic, 1)

				qStart := time.Now()
				var err error
				if cfg != nil {
					polyline := s2.PolylineFromLatLngs(road.lls)
					covering := cfg.Covering(polyline)
					if covering == nil {
						continue
					}
					_, err = db.Exec(fmt.Sprintf(
						`INSERT INTO roads (id, geometry) VALUES (unique_rowid(), '%s'); INSERT INTO roads_s2_idx VALUES (unique_rowid(), '%s');`,
						road.geometry, covering[0].String(),
					))
				} else {
					_, err = db.Exec(fmt.Sprintf(
						`INSERT INTO roads (id, geometry) VALUES (%d, '%s');`,
						id, road.geometry,
					))
				}
				qDuration := time.Since(qStart)
				if err == errQuerySkipped {
					continue
				} else if err != nil {
					return err
				}
				_ = qDuration

				{
					now := time.Now()
					running := now.Sub(start)
					resultsMu.Lock()
					resultsMu.nanosHist.RecordValue(qDuration.Nanoseconds())
					// resultsMu.countsHist.RecordValue(count)
					if now.Sub(resultsMu.lastUpdate) > updateInterval {
						resultsMu.lastUpdate = now
						fmt.Printf("finished %d queries in %s\n", resultsMu.queries, running)
					}
					resultsMu.Unlock()
					if running > throughputMaxDuration {
						cancel()
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	fmt.Printf("finished inserts shape %s concurrency %d\n", queryShape, concurrency)
	fmt.Println("concurrency__meters_____numQ_pMin(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__count50__count95__count99_countMax")
	resultsMu.Lock()
	defer resultsMu.Unlock()
	nanosHist, countsHist := resultsMu.nanosHist, resultsMu.countsHist
	if nanosHist.TotalCount() == 0 {
		return nil
	}
	fmt.Printf("%11d %7d %8d %8.2f %8.2f %8.2f %8.2f %8.2f %8d %8d %8d %8d\n",
		concurrency,
		int(metersFromLevel(level)),
		nanosHist.TotalCount(),
		time.Duration(nanosHist.Min()).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(nanosHist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(nanosHist.Max()).Seconds()*1000,
		countsHist.ValueAtQuantile(50),
		countsHist.ValueAtQuantile(95),
		countsHist.ValueAtQuantile(99),
		countsHist.Max(),
	)

	return nil
}

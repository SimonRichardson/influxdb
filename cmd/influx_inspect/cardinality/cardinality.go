package cardinality

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"text/tabwriter"

	"github.com/influxdata/influxdb/cmd/influx_inspect/report"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/reporthelper"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influxd cardinality".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dbPath     string
	shardPaths map[uint64]string
	exact      bool
	// How many goroutines to dedicate to calculating cardinality.
	concurrency int
}

// NewCommand returns a new instance of Command with default setting applied.
func NewCommand() *Command {
	return &Command{
		Stderr:      os.Stderr,
		Stdout:      os.Stdout,
		shardPaths:  map[uint64]string{},
		concurrency: runtime.GOMAXPROCS(0),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) (err error) {
	fs := flag.NewFlagSet("cardinality", flag.ExitOnError)
	fs.StringVar(&cmd.dbPath, "db-path", "", "Path to databaseRetentionPolicies. Required.")
	fs.IntVar(&cmd.concurrency, "c", runtime.GOMAXPROCS(0), "Set worker concurrency. Defaults to GOMAXPROCS setting.")
	fs.BoolVar(&cmd.exact, "exact", false, "Report exact counts")

	fs.SetOutput(cmd.Stdout)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.dbPath == "" {
		return errors.New("path to databaseRetentionPolicies must be provided")
	}

	dbMap := make(databaseRetentionPolicies)

	// Walk databaseRetentionPolicies directory to get shards.
	if err := filepath.Walk(cmd.dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		// TODO(edd): this would be a problem if the retention policy was named
		// "index".
		if info.Name() == tsdb.SeriesFileDirectory || info.Name() == "index" {
			return filepath.SkipDir
		}

		id, err := strconv.Atoi(info.Name())
		if err != nil {
			return nil
		}
		cmd.shardPaths[uint64(id)] = path
		return nil
	}); err != nil {
		return err
	}

	if len(cmd.shardPaths) == 0 {
		_, err := fmt.Fprintf(cmd.Stderr, "No shards under %s\n", cmd.dbPath)
		return err
	}

	estTitle := " (estimated)"
	newCounterFn := report.NewHLLCounter
	if cmd.exact {
		newCounterFn = report.NewExactCounter
		estTitle = ""
	}

	for _, p := range cmd.shardPaths {
		err := reporthelper.WalkShardDirs(p, func(db, rp, id, path string) error {
			file, err := os.OpenFile(path, os.O_RDONLY, 0600)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", path, err)
				return nil
			}

			reader, err := tsm1.NewTSMReader(file)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
				_ = file.Close()
				return nil
			}
			seriesCount := reader.KeyCount()
			for i := 0; i < seriesCount; i++ {
				key, _ := reader.KeyAt(i)
				seriesKey, field, _ := bytes.Cut(key, []byte("#!~#"))
				measurement, _ := models.ParseKey(seriesKey)
				fs := initFieldsAndSeries(dbMap, db, rp, measurement, newCounterFn)
				fs.series.Add(key)
				fs.fields.Add(field)
			}
			if err := reader.Close(); err != nil {
				fmt.Fprintf(cmd.Stderr, "error closing: %s: %v.\n", file.Name(), err)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "%s: %v\n", p, err)
			return err
		}
	}
	tw := tabwriter.NewWriter(cmd.Stdout, 8, 2, 1, ' ', 0)
	c2Cardinality := uint64(0)
	seriesTotal := uint64(0)
	fieldsTotal := uint64(0)

	if _, err = fmt.Fprintln(tw, "measurement\tseries\tfields\tcloud2 cardinality"); err != nil {
		return err
	}
	if _, err = fmt.Fprintln(tw, "-----------\t------\t------\t------------------"); err != nil {
		return err
	}

	for d, db := range dbMap {
		for r, rp := range db {
			for m, measure := range rp {
				seriesN := measure.series.Count()
				fieldsN := measure.fields.Count()
				_, err = fmt.Fprintf(tw, "%q.%q.%q\t%d\t%d\t%d\n",
					d,
					r,
					m,
					seriesN,
					fieldsN,
					seriesN*fieldsN)
				if err != nil {
					return err
				}
				c2Cardinality += seriesN * fieldsN
				seriesTotal += seriesN
				fieldsTotal += fieldsN
			}
		}
	}
	if _, err = fmt.Fprintln(tw, "-----------\t------\t------\t------------------"); err != nil {
		return err
	}
	_, err = fmt.Fprintf(tw, "total%s\t%d\t%d\t%d\n", estTitle, seriesTotal, fieldsTotal, c2Cardinality)
	if err != nil {
		return err
	}
	return tw.Flush()
}

type fieldsAndSeries struct {
	fields report.Counter
	series report.Counter
}
type measurementFieldsAndSeries map[string]*fieldsAndSeries
type retentionPolicyMeasurements map[string]measurementFieldsAndSeries
type databaseRetentionPolicies map[string]retentionPolicyMeasurements

func initFieldsAndSeries(toto databaseRetentionPolicies, db, rp, ms string, fn func() report.Counter) *fieldsAndSeries {
	rpMap, ok := toto[db]
	if !ok {
		rpMap = make(retentionPolicyMeasurements)
		toto[db] = rpMap
	}
	measurementMap, ok := rpMap[rp]
	if !ok {
		measurementMap = make(measurementFieldsAndSeries)
		rpMap[rp] = measurementMap
	}
	m, ok := measurementMap[ms]
	if !ok {
		m = &fieldsAndSeries{fields: fn(), series: fn()}
		measurementMap[ms] = m
	}
	return m
}

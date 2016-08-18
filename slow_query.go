package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var patterns map[string]*regexp.Regexp = map[string]*regexp.Regexp{
	"header":   regexp.MustCompile("(^Tcp port:|^Time\\s+Id|started with:$)"),
	"skip":     regexp.MustCompile("^(# Time: \\d+|use \\w+;)"),
	"userhost": regexp.MustCompile("^# User@Host: (\\S+)\\[\\S+\\] @\\s+\\[(\\S+)\\]"),
	"thread":   regexp.MustCompile("^# Thread_id: (\\d+)\\s+Schema: (\\w+)\\s+Last_errno: (\\d+)\\s+Killed: (\\d+)"),
	"qtime":    regexp.MustCompile("^# Query_time: (\\S+)\\s+Lock_time: (\\S+)\\s+Rows_sent: (\\d+)\\s+Rows_examined: (\\d+)\\s+Rows_affected: (\\d+)\\s+Rows_read: (\\d+)"),
	"bytes":    regexp.MustCompile("^# Bytes_sent: (\\d+)\\s+Tmp_tables: (\\d+)\\s+Tmp_disk_tables: (\\d+)\\s+Tmp_table_sizes: (\\d+)"),
	"trxid":    regexp.MustCompile("^# InnoDB_trx_id: (\\S+)"),
	"qchit":    regexp.MustCompile("^# QC_Hit: (\\w+)\\s+Full_scan: (\\w+)\\s+Full_join: (\\w+)"),
	"filesort": regexp.MustCompile("^# Filesort: (\\w+)\\s+Filesort_on_disk: (\\w+)\\s+Merge_passes: (\\d+)"),
	"innoio":   regexp.MustCompile("^#\\s+InnoDB_IO_r_ops: (\\d+)\\s+InnoDB_IO_r_bytes: (\\d+)\\s+InnoDB_IO_r_wait: (\\S+)"),
	"innowait": regexp.MustCompile("^#\\s+InnoDB_rec_lock_wait: (\\S+)\\s+InnoDB_queue_wait: (\\S+)"),
	"innopage": regexp.MustCompile("^#\\s+InnoDB_pages_distinct: (\\d+)"),
	"time":     regexp.MustCompile("^SET timestamp=(\\d+)"),
	"query":    regexp.MustCompile("^(SELECT|INSERT|UPDATE|DELETE)\\b"),
}

type SlowQuery struct {
	User            string
	Host            string
	Time            time.Time
	ThreadId        uint32
	Schema          string
	Errno           uint32
	Killed          bool
	QueryTime       float64
	LockTime        float64
	RowsSent        uint64
	RowsExamined    uint64
	RowsAffected    uint64
	RowsRead        uint64
	BytesSent       uint64
	TmpTables       uint32
	TmpDiskTables   uint32
	TmpTableSize    uint64
	TransactionId   string
	UsedQueryCache  bool
	FullScan        bool
	FullJoin        bool
	Filesort        bool
	FilesortOnDisk  bool
	MergePasses     uint32
	IoReadOps       uint64
	IoReadBytes     uint64
	IoReadWait      float64
	IoLockWait      float64
	IoQueueWait     float64
	IoDistinctPages uint64
	Query           string
}

// Parses the slow query log a line at a time. We decide we're done once
// we've seen a line that looks like a SQL query.
func (query *SlowQuery) ParseLine(line string) (done bool, err error) {
	if patterns["header"].MatchString(line) || patterns["skip"].MatchString(line) {
		return false, nil
	}

	matches := patterns["userhost"].FindStringSubmatch(line)
	if matches != nil {
		query.User = matches[1]
		query.Host = matches[2]
		return false, nil
	}

	matches = patterns["thread"].FindStringSubmatch(line)
	if matches != nil {
		query.ThreadId = parseUint32(matches[1])
		query.Schema = matches[2]
		query.Errno = parseUint32(matches[3])
		query.Killed = false
		if parseUint32(matches[4]) > 0 {
			query.Killed = true
		}
		return false, nil
	}

	matches = patterns["qtime"].FindStringSubmatch(line)
	if matches != nil {
		query.QueryTime = parseFloat(matches[1])
		query.LockTime = parseFloat(matches[2])
		query.RowsSent = parseUint64(matches[3])
		query.RowsExamined = parseUint64(matches[4])
		query.RowsAffected = parseUint64(matches[5])
		query.RowsRead = parseUint64(matches[6])
		return false, nil
	}

	matches = patterns["bytes"].FindStringSubmatch(line)
	if matches != nil {
		query.BytesSent = parseUint64(matches[1])
		query.TmpTables = parseUint32(matches[2])
		query.TmpDiskTables = parseUint32(matches[3])
		query.TmpTableSize = parseUint64(matches[4])
		return false, nil
	}

	matches = patterns["trxid"].FindStringSubmatch(line)
	if matches != nil {
		query.TransactionId = matches[1]
		return false, nil
	}

	matches = patterns["qchit"].FindStringSubmatch(line)
	if matches != nil {
		query.UsedQueryCache = parseBoolean(matches[1])
		query.FullScan = parseBoolean(matches[2])
		query.FullJoin = parseBoolean(matches[3])
		return false, nil
	}

	matches = patterns["filesort"].FindStringSubmatch(line)
	if matches != nil {
		query.Filesort = parseBoolean(matches[1])
		query.FilesortOnDisk = parseBoolean(matches[2])
		query.MergePasses = parseUint32(matches[3])
		return false, nil
	}

	matches = patterns["innoio"].FindStringSubmatch(line)
	if matches != nil {
		query.IoReadOps = parseUint64(matches[1])
		query.IoReadBytes = parseUint64(matches[2])
		query.IoReadWait = parseFloat(matches[3])
		return false, nil
	}

	matches = patterns["innowait"].FindStringSubmatch(line)
	if matches != nil {
		query.IoLockWait = parseFloat(matches[1])
		query.IoQueueWait = parseFloat(matches[2])
		return false, nil
	}

	matches = patterns["innopage"].FindStringSubmatch(line)
	if matches != nil {
		query.IoDistinctPages = parseUint64(matches[1])
		return false, nil
	}

	matches = patterns["time"].FindStringSubmatch(line)
	if matches != nil {
		timestamp, _ := strconv.ParseInt(matches[1], 10, 64)
		query.Time = time.Unix(timestamp, 0)
		return false, nil
	}

	matches = patterns["query"].FindStringSubmatch(line)
	if matches != nil {
		query.Query = line
		return true, nil
	}

	// TODO: Get query fingerprint by calling pt-query-digest.
	// Better idea: Extract the fingerprint code from pt-query-digest.

	return false, errors.New(fmt.Sprintf("Weird-looking log line:\n%s\n", line))
}

func parseUint32(str string) uint32 {
	u64, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(u64)
}

func parseUint64(str string) uint64 {
	u64, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		panic(err)
	}
	return u64
}

func parseFloat(str string) float64 {
	f64, err := strconv.ParseFloat(str, 10)
	if err != nil {
		panic(err)
	}
	return f64
}

func parseBoolean(word string) bool {
	if word == "No" {
		return false
	} else if word == "Yes" {
		return true
	}
	panic(fmt.Sprintf("Unexpected boolean string: '%s'", word))
}

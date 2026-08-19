package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kvdb/kvdb/internal/netutil"
)

// copyPartitionSnapshot reseeds one partition from its master onto another node
// by streaming the master's snapshot of that partition (spec 4 and 12.5: a
// replica that has no base backup asks the leader for a snapshot).
//
// The master's partition WAL sequence travels with the snapshot in the
// X-Partition-WAL-Seq header. Without it the target would restart its
// replication bookkeeping at zero and treat the very next streamed entry as a
// gap spanning the master's entire history.
//
// This lives here because three call sites need it - a replication-factor
// increase, a recovered node, and the collapse at the end of a reshard - and
// three copies of it is three places for the header handling to drift.
func copyPartitionSnapshot(client *http.Client, partitionID int, master *NodeInfo, target *NodeInfo) error {
	if master == nil || target == nil {
		return fmt.Errorf("copyPartitionSnapshot: nil node")
	}
	if master.ID == target.ID {
		return nil
	}

	timeout := client.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	getURL := fmt.Sprintf("http://%s:%d/snapshot?partition_id=%d", master.Address, master.Port, partitionID)
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return err
	}
	getResp, err := client.Do(getReq)
	if err != nil {
		return err
	}
	defer netutil.DrainAndClose(getResp)
	if getResp.StatusCode != http.StatusOK {
		return fmt.Errorf("master snapshot returned %d", getResp.StatusCode)
	}
	walSeqHeader := getResp.Header.Get("X-Partition-WAL-Seq")
	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		return err
	}

	postURL := fmt.Sprintf("http://%s:%d/snapshot?partition_id=%d", target.Address, target.Port, partitionID)
	if walSeqHeader != "" {
		postURL += "&wal_seq=" + walSeqHeader
	}
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	postReq.Header.Set("Content-Type", "application/octet-stream")

	postResp, err := client.Do(postReq)
	if err != nil {
		return err
	}
	defer netutil.DrainAndClose(postResp)
	if postResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(postResp.Body)
		return fmt.Errorf("target snapshot apply returned %d: %s", postResp.StatusCode, string(body))
	}
	return nil
}

package netutil

import (
	"io"
	"net/http"
)

// DrainAndClose drains a (typically small) HTTP response body and then closes it.
// Draining enables the underlying TCP connection to be reused by net/http.
func DrainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

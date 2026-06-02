package pgfox

import (
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// msgBodyPool — reusable byte slices for PostgreSQL protocol message bodies.
//
// Lifetime contract: a slice borrowed from this Pool is valid only for the
// synchronous duration of the ReadMessage call that fills it. Callers that
// need the body to outlive a single message dispatch MUST copy the relevant
// bytes out (or call cloneMsgBody) before returning the slice.
//
// Bucket strategy: two size classes keep fragmentation low and avoid giving
// a 256 MiB max-message-size slot for a 40-byte ping.
//   small  — cap 4 KiB  : startup messages, short queries, command-complete
//   large  — cap 64 KiB : typical data rows, bind parameters, COPY payloads
//
// Messages larger than 64 KiB are allocated directly from the heap and never
// pooled, because they are rare and holding a giant buffer in the Pool would
// waste memory across the idle lifetime of that goroutine's P.
// ---------------------------------------------------------------------------

const (
	msgBodySmallCap = 4 * 1024
	msgBodyLargeCap = 64 * 1024
	msgBodyPoolMax  = msgBodyLargeCap // bodies above this skip the Pool
)

var msgBodySmallPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, msgBodySmallCap)
		return &b
	},
}

var msgBodyLargePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, msgBodyLargeCap)
		return &b
	},
}

// getMsgBody returns a *[]byte from the appropriate Pool, reset to length 0.
// The caller must call putMsgBody when done.
func getMsgBody(need int) *[]byte {
	if need <= msgBodySmallCap {
		return msgBodySmallPool.Get().(*[]byte)
	}
	return msgBodyLargePool.Get().(*[]byte)
}

// putMsgBody returns a pooled slice. Only call with slices obtained from
// getMsgBody; do not put slices that still have live references.
func putMsgBody(p *[]byte) {
	b := *p
	if cap(b) > msgBodyPoolMax {
		// Oversized — let it be GC'd.
		return
	}
	*p = b[:0]
	if cap(b) <= msgBodySmallCap {
		msgBodySmallPool.Put(p)
	} else {
		msgBodyLargePool.Put(p)
	}
}

// ---------------------------------------------------------------------------
// pipelinePool — reusable []pipelineMsg slices for the extended query path.
//
// A typical pipeline is 4 messages (Parse + Bind + Execute + Sync). The Pool
// keeps a backing slice of capacity 16 to handle deeper pipelines without
// reallocation. executeExtendedPipeline borrows one slice for `pipeline` and
// one for `rewritten`, returns both before returning to the caller.
// ---------------------------------------------------------------------------

const pipelineInitCap = 16

var pipelinePool = sync.Pool{
	New: func() any {
		s := make([]pipelineMsg, 0, pipelineInitCap)
		return &s
	},
}

func getPipeline() *[]pipelineMsg {
	return pipelinePool.Get().(*[]pipelineMsg)
}

func putPipeline(p *[]pipelineMsg) {
	*p = (*p)[:0]
	pipelinePool.Put(p)
}

// ---------------------------------------------------------------------------
// paramWorkspace — reusable scratch space for ClassifyAndParameterize.
//
// Every simple-query dispatch that hits the parameterization path allocates:
//   - []literalValue  (AST walk scratch)
//   - strings.Builder (SQL rewrite)
//   - []string        (extracted parameter values)
//
// Pooling a workspace struct amortises all three allocations across calls.
// The workspace is borrowed at the top of ClassifyAndParameterize and
// returned after rewriteLiterals produces its output strings, which are then
// transferred to the heap-allocated ParameterizeResult before the workspace
// is put back.
// ---------------------------------------------------------------------------

type paramWorkspace struct {
	litValues []literalValue
	values    []string
	sb        strings.Builder
}

var paramWorkspacePool = sync.Pool{
	New: func() any {
		return &paramWorkspace{
			litValues: make([]literalValue, 0, 16),
			values:    make([]string, 0, 16),
		}
	},
}

func getParamWorkspace() *paramWorkspace {
	return paramWorkspacePool.Get().(*paramWorkspace)
}

func putParamWorkspace(w *paramWorkspace) {
	w.litValues = w.litValues[:0]
	w.values = w.values[:0]
	w.sb.Reset()
	paramWorkspacePool.Put(w)
}

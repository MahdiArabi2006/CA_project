package victimcache

import (
	"github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/timing"
)

type Spec struct {
	Freq       timing.Freq
	NumEntries uint64
	BlockSize  uint64
	HitLatency int
}

type VCEntry struct {
	Tag       uint64
	Data      []byte
	Valid     bool
	Dirty	  bool
	AccessSeq uint64 
}

type State struct {
	Entries  []VCEntry
	SeqCount uint64 
	PendingReads map[uint64]*memprotocol.ReadReq
}

type Resources struct{}

type Comp = modeling.Component[Spec, State, Resources]

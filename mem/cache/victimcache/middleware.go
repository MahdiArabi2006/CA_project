package victimcache

import (
	"github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/messaging"
	"github.com/sarchlab/akita/v5/timing"
)

type vcMiddleware struct {
	comp *Comp
}

func (m *vcMiddleware) Tick() bool {
	madeProgress := false
	madeProgress = m.processTopPort() || madeProgress
	madeProgress = m.processBottomPort() || madeProgress
	return madeProgress
}

func (m *vcMiddleware) processTopPort() bool {
	topPort := m.comp.GetPortByName("Top")
	msg := topPort.PeekIncoming()
	if msg == nil {
		return false
	}

	switch req := msg.(type) {
	case memprotocol.ReadReq:
		return m.handleReadReq(&req, topPort)
	case memprotocol.WriteReq:
		return m.handleWriteReq(&req, topPort)
	}
	return false
}

func (m *vcMiddleware) handleReadReq(req *memprotocol.ReadReq, topPort messaging.Port) bool {
	tag := m.getBlockTag(req.Address)
	idx := m.lookup(tag)

	// hit in victim cache
	if idx >= 0 {
		bottomPort := m.comp.GetPortByName("Bottom")

		if m.comp.State.Entries[idx].Dirty {
			if !topPort.CanSend() || !bottomPort.CanSend() {
				return false
			}

			victimEntry := m.comp.State.Entries[idx]
			victimAddr := victimEntry.Tag << m.blockOffsetBits()
			dstPort := m.comp.Resources().AddressToPortMapper.Find(victimAddr)

			wbReq := memprotocol.WriteReq{
				MsgMeta: messaging.MsgMeta{
					ID:  timing.GetIDGenerator().Generate(),
					Src: bottomPort.AsRemote(),
					Dst: dstPort,
				},
				Address:   victimAddr,
				PID:       req.PID,
				Data:      victimEntry.Data,
			}
			bottomPort.Send(wbReq)
		} else {
			if !topPort.CanSend() {
				return false
			}
		}

		offset := req.Address % uint64(m.comp.Spec().BlockSize)
		dataSize := req.AccessByteSize
		data := m.comp.State.Entries[idx].Data[offset : offset+uint64(dataSize)]

		rsp := memprotocol.DataReadyRsp{
			MsgMeta: messaging.MsgMeta{
				ID:    timing.GetIDGenerator().Generate(),
				Src:   topPort.AsRemote(),
				Dst:   req.Src,
				RspTo: req.ID,
			},
			Data: data,
		}

		topPort.Send(rsp)
		topPort.RetrieveIncoming()
		m.comp.State.Entries[idx].Valid = false
		return true
	}

	// miss in victim cache
	bottomPort := m.comp.GetPortByName("Bottom")
	dstPort := m.comp.Resources().AddressToPortMapper.Find(req.Address)

	forwardReq := memprotocol.ReadReq{
		MsgMeta: messaging.MsgMeta{
			ID:  timing.GetIDGenerator().Generate(),
			Src: bottomPort.AsRemote(),
			Dst: dstPort,
		},
		Address:        req.Address,
		AccessByteSize: req.AccessByteSize,
		PID:            req.PID,
	}

	if bottomPort.CanSend() {
		bottomPort.Send(forwardReq)
		m.comp.State.PendingReads[forwardReq.ID] = *req
		topPort.RetrieveIncoming()
		return true
	}

	return false
}

func (m *vcMiddleware) handleWriteReq(req *memprotocol.WriteReq, topPort messaging.Port) bool {
	tag := m.getBlockTag(req.Address)
	idx := m.lookup(tag)
	blockSize := m.comp.Spec().BlockSize
	bottomPort := m.comp.GetPortByName("Bottom")

	// Bypass to L2 for partial writes
	if idx == -1 && len(req.Data) < int(blockSize) {
		if !bottomPort.CanSend() || !topPort.CanSend() {
			return false
		}

		dstPort := m.comp.Resources().AddressToPortMapper.Find(req.Address)
		forwardReq := memprotocol.WriteReq{
			MsgMeta: messaging.MsgMeta{
				ID:  timing.GetIDGenerator().Generate(),
				Src: bottomPort.AsRemote(),
				Dst: dstPort,
			},
			Address: req.Address,
			PID:     req.PID,
			Data:    req.Data,
		}
		bottomPort.Send(forwardReq)

		rsp := memprotocol.WriteDoneRsp{
			MsgMeta: messaging.MsgMeta{
				ID:    timing.GetIDGenerator().Generate(),
				Src:   topPort.AsRemote(),
				Dst:   req.Src,
				RspTo: req.ID,
			},
		}
		topPort.Send(rsp)
		topPort.RetrieveIncoming()
		return true
	}

	needsWriteBack := false
	if idx == -1 {
		idx, needsWriteBack = m.findVictimLRU()
	}

	if !topPort.CanSend() {
		return false
	}
	if needsWriteBack && m.comp.State.Entries[idx].Dirty {
		if !bottomPort.CanSend() {
			return false
		}
	}

	if needsWriteBack && m.comp.State.Entries[idx].Dirty {
		victimEntry := m.comp.State.Entries[idx]
		victimAddr := victimEntry.Tag << m.blockOffsetBits()
		dstPort := m.comp.Resources().AddressToPortMapper.Find(victimAddr)

		wbReq := memprotocol.WriteReq{
			MsgMeta: messaging.MsgMeta{
				ID:  timing.GetIDGenerator().Generate(),
				Src: bottomPort.AsRemote(),
				Dst: dstPort,
			},
			Address:   victimAddr,
			PID:       req.PID,
			Data:      victimEntry.Data,
		}
		bottomPort.Send(wbReq)
	}

	rsp := memprotocol.WriteDoneRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   topPort.AsRemote(),
			Dst:   req.Src,
			RspTo: req.ID,
		},
	}
	topPort.Send(rsp)

	var blockData []byte
	if m.comp.State.Entries[idx].Valid && m.comp.State.Entries[idx].Tag == tag {
		blockData = m.comp.State.Entries[idx].Data
	} else {
		blockData = make([]byte, blockSize)
	}

	offset := req.Address % uint64(blockSize)
	for i := 0; i < len(req.Data); i++ {
		blockData[offset+uint64(i)] = req.Data[i]
	}

	m.comp.State.SeqCount++
	m.comp.State.Entries[idx] = VCEntry{
		Tag:       tag,
		Data:      blockData,
		Valid:     true,
		Dirty:     true,
		AccessSeq: m.comp.State.SeqCount,
	}

	topPort.RetrieveIncoming()
	return true
}


func (m *vcMiddleware) processBottomPort() bool {
	bottomPort := m.comp.GetPortByName("Bottom")
	topPort := m.comp.GetPortByName("Top")

	msg := bottomPort.PeekIncoming()
	if msg == nil {
		return false
	}

	switch rsp := msg.(type) {
	case memprotocol.DataReadyRsp:
		// finging L1 request
		origReq, exists := m.comp.State.PendingReads[rsp.RspTo]
		if !exists {
			panic("response not found in pending reads!")
		}

		topRsp := memprotocol.DataReadyRsp{
			MsgMeta: messaging.MsgMeta{
				ID:    timing.GetIDGenerator().Generate(),
				Src:   topPort.AsRemote(),
				Dst:   origReq.Src,
				RspTo: origReq.ID,
			},
			Data: rsp.Data,
		}

		if topPort.CanSend() {
			topPort.Send(topRsp)
			bottomPort.RetrieveIncoming()
			delete(m.comp.State.PendingReads, rsp.RspTo)
			return true
		}
	case memprotocol.WriteDoneRsp:
		// no need to forward writeback rsp
		bottomPort.RetrieveIncoming()
		return true
	}

	return false
}

func (m *vcMiddleware) lookup(tag uint64) int {
	for i, entry := range m.comp.State.Entries {
		if entry.Valid && entry.Tag == tag {
			return i
		}
	}
	return -1
}

func (m *vcMiddleware) findVictimLRU() (int, bool) {
	for i, entry := range m.comp.State.Entries {
		if !entry.Valid {
			return i, false
		}
	}

	lruIdx := 0
	minSeq := m.comp.State.Entries[0].AccessSeq

	for i := 1; i < len(m.comp.State.Entries); i++ {
		if m.comp.State.Entries[i].AccessSeq < minSeq {
			minSeq = m.comp.State.Entries[i].AccessSeq
			lruIdx = i
		}
	}
	return lruIdx, true
}

func (m *vcMiddleware) getBlockTag(addr uint64) uint64 {
	return addr >> m.blockOffsetBits()
}

func (m *vcMiddleware) blockOffsetBits() uint64 {
	size := m.comp.Spec().BlockSize
	var bits uint64 = 0
	for size > 1 {
		bits++
		size >>= 1
	}
	return bits
}

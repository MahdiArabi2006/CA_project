package victimcache

import (
	"github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/timing"
)

var defaultSpec = Spec{
	Freq:       1 * timing.GHz, 
	NumEntries: 8,              
	BlockSize:  64,             
	HitLatency: 1,
	maxEvictionBufferSize: 8,           
}

func DefaultSpec() Spec {
	return defaultSpec
}

type Builder struct {
	spec      Spec
	registrar modeling.Registrar
	resources Resources
}

func MakeBuilder() Builder {
	return Builder{
		spec: Spec{
			Freq:       1 * timing.GHz,
			NumEntries: 8,
			BlockSize:  64,
			HitLatency: 1,
			maxEvictionBufferSize: 8,
		},
	}
}

func (b Builder) WithSpec(spec Spec) Builder {
	b.spec = spec
	return b
}

func (b Builder) WithRegistrar(r modeling.Registrar) Builder {
	b.registrar = r
	return b
}

func (b Builder) WithResources(r Resources) Builder {
    b.resources = r
    return b
}

func (b Builder) Build(name string) *Comp {
	if b.registrar == nil {
		panic("victimcache: WithRegistrar is required")
	}

	comp := modeling.NewBuilder[Spec, State, Resources]().
		WithEngine(b.registrar.GetEngine()).
		WithFreq(b.spec.Freq).
		WithSpec(b.spec).
		WithResources(b.resources).
		Build(name)

	comp.State = State{
		Entries:  make([]VCEntry, b.spec.NumEntries),
		SeqCount: 0,
		PendingReads: make(map[uint64]memprotocol.ReadReq),
	}

	comp.DeclarePort("Top", memprotocol.Responder)
	comp.DeclarePort("Bottom", memprotocol.Requester)
	comp.DeclarePort("Control", memprotocol.Responder)

	mw := &vcMiddleware{comp: comp}
	comp.AddMiddleware(mw)

	b.registrar.RegisterComponent(comp)

	return comp
}

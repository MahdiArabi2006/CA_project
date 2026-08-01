package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/sarchlab/akita/v5/benchmarks/trashingmemaccessagent"
	"github.com/sarchlab/akita/v5/mem"
	"github.com/sarchlab/akita/v5/mem/cache/victimcache"
	"github.com/sarchlab/akita/v5/mem/cache/writeback"
	"github.com/sarchlab/akita/v5/mem/idealmemcontroller"
	"github.com/sarchlab/akita/v5/messaging"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/noc/directconnection"

	"github.com/sarchlab/akita/v5/simulation"
	"github.com/sarchlab/akita/v5/timing"
)

var seedFlag = flag.Int64("seed", 0, "Random Seed")
var numAccessFlag = flag.Int("num-access", 10000,
	"Number of accesses to generate")
var maxAddressFlag = flag.Uint64("max-address", 1048576, "Address range to use")
var parallelFlag = flag.Bool("parallel", false, "Test with parallel engine")
var traceFlag = flag.Bool("trace", false, "Collect trace")

func buildEnvironment() (*simulation.Simulation, timing.Engine, *trashingmemaccessagent.MemAccessAgent, *writeback.Comp, *victimcache.Comp) {
	simBuilder := simulation.MakeBuilder()

	if *parallelFlag {
		simBuilder = simBuilder.WithParallelEngine()
	}
	if *traceFlag {
		simBuilder = simBuilder.WithVisTracingOnStart()
	}

	s := simBuilder.Build()
	engine := s.GetEngine()

	conn := directconnection.MakeBuilder().
		WithRegistrar(s).
		Build("Conn")

	agentSpec := trashingmemaccessagent.DefaultSpec()
	agentSpec.MaxAddress = *maxAddressFlag
	agentSpec.WriteLeft = *numAccessFlag
	agentSpec.ReadLeft = *numAccessFlag
	agentSpec.CacheSize = 4096
	agent := trashingmemaccessagent.MakeBuilder().
		WithRegistrar(s).
		WithSpec(agentSpec).
		Build("MemAccessAgent")
	assignPorts(s, agent, "Mem")
	createProgressBars(s, agent)

	dram := buildDRAM(s)

	vcToDramMapper := new(mem.SinglePortMapper)
	vcToDramMapper.Port = dram.GetPortByName("Top").AsRemote()

	vcSpec := victimcache.DefaultSpec()

	victimCache := victimcache.MakeBuilder().
		WithRegistrar(s).
		WithSpec(vcSpec).
		WithResources(victimcache.Resources{
			AddressToPortMapper: vcToDramMapper,
		}).
		Build("VictimCache")

	assignPorts(s, victimCache, "Top", "Bottom", "Control")

	l1ToVcMapper := new(mem.SinglePortMapper)
	l1ToVcMapper.Port = victimCache.GetPortByName("Top").AsRemote()

	cacheSpec := writeback.DefaultSpec()
	cacheSpec.TotalByteSize = 4096
	cacheSpec.Log2BlockSize = 6
	// Direct-map cache
	cacheSpec.WayAssociativity = 1
	cacheSpec.NumMSHREntry = 4
	cacheSpec.NumReqPerCycle = 16

	writeBackCache := writeback.MakeBuilder().
		WithRegistrar(s).
		WithSpec(cacheSpec).
		WithResources(writeback.Resources{
			AddressToPortMapper: l1ToVcMapper,
		}).
		Build("Cache")
	assignPorts(s, writeBackCache, "Top", "Bottom", "Control")

	agent.LowModule = writeBackCache.GetPortByName("Top")

	conn.PlugIn(agent.GetPortByName("Mem"))
	conn.PlugIn(writeBackCache.GetPortByName("Bottom"))
	conn.PlugIn(writeBackCache.GetPortByName("Top"))
	conn.PlugIn(victimCache.GetPortByName("Top"))
	conn.PlugIn(victimCache.GetPortByName("Bottom"))
	conn.PlugIn(dram.GetPortByName("Top"))

	return s, engine, agent, writeBackCache, victimCache
}

// buildDRAM builds and registers the backing ideal memory controller.
func buildDRAM(s *simulation.Simulation) *idealmemcontroller.Comp {
	dramSpec := idealmemcontroller.DefaultSpec()
	dramSpec.Capacity = 4 * mem.GB
	dram := idealmemcontroller.MakeBuilder().
		WithRegistrar(s).
		WithSpec(dramSpec).
		Build("DRAM")
	assignPorts(s, dram, "Top", "Control")

	return dram
}

// assignPorts builds a port for each declared name on the component and assigns
// it, choosing a default buffer size.
func assignPorts(
	s *simulation.Simulation,
	comp messaging.Component,
	names ...string,
) {
	for _, name := range names {
		p := modeling.MakePortBuilder().
			WithRegistrar(s).
			WithComponent(comp).
			WithSpec(modeling.PortSpec{BufSize: 16}).
			Build(name)
		comp.AssignPort(name, p)
	}
}

func createProgressBars(
	s *simulation.Simulation,
	agent *trashingmemaccessagent.MemAccessAgent,
) {
	if monitor := s.GetMonitor(); monitor != nil {
		agent.CreateProgressBars(monitor.CreateProgressBar)
	}
}

func main() {
	flag.Parse()

	var seed int64
	if *seedFlag == 0 {
		seed = time.Now().UnixNano()
	} else {
		seed = *seedFlag
	}

	fmt.Fprintf(os.Stderr, "Seed %d\n", seed)
	rand.Seed(seed)

	s, engine, agent, cache, victimcache := buildEnvironment()
	agent.TickLater()

	err := engine.Run()
	if err != nil {
		panic(err)
	}

	if len(agent.State.PendingWriteReq) > 0 || len(agent.State.PendingReadReq) > 0 {
		panic("Not all req returned")
	}

	if agent.State.WriteLeft > 0 || agent.State.ReadLeft > 0 {
		panic("more requests to send")
	}

	fmt.Println("\n=================== CACHE STATISTICS & AMAT ===================")

	l1Hits := cache.State.StatHitCount
	l1Misses := cache.State.StatMissCount
	l1Accesses := l1Hits + l1Misses

	vcHits := victimcache.State.StatHitCount
	vcMisses := victimcache.State.StatMissCount
	vcAccesses := vcHits + vcMisses

	L2ReadTraffic := victimcache.State.StatL2ReadTraffic
	L2WriteTraffic := victimcache.State.StatL2WriteTraffic

	var l1HitRate, l1MissRate float64
	if l1Accesses > 0 {
		l1HitRate = float64(l1Hits) / float64(l1Accesses)
		l1MissRate = float64(l1Misses) / float64(l1Accesses)
	}

	var vcHitRate, vcMissRate float64
	if vcAccesses > 0 {
		vcHitRate = float64(vcHits) / float64(vcAccesses)
		vcMissRate = float64(vcMisses) / float64(vcAccesses)
	}


	hitTimeL1 := 1.0
	penaltyVC := 1.0 // Latency of Victim Cache
	penaltyL2 := 100.0 // Latency of Main Memory/DRAM

	amatWithVC := hitTimeL1 + l1MissRate*(vcHitRate*penaltyVC+vcMissRate*penaltyL2)

	amatBase := hitTimeL1 + l1MissRate*penaltyL2

	fmt.Printf("Total L1 Accesses : %d\n", l1Accesses)
	fmt.Printf("L1 Hit Rate       : %.2f%%\n", l1HitRate*100)
	fmt.Printf("L1 Miss Rate      : %.2f%%\n", l1MissRate*100)
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("Victim Cache Accesses : %d\n", vcAccesses)
	fmt.Printf("VC Hit Rate           : %.2f%%\n", vcHitRate*100)
	fmt.Printf("VC Miss Rate          : %.2f%%\n", vcMissRate*100)
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("Total L2 Read Traffic : %d Bytes\n", L2ReadTraffic)
	fmt.Printf("Total L2 Write Traffic: %d Bytes\n", L2WriteTraffic)
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("AMAT (Base - No VC)   : %.2f Cycles\n", amatBase)
	fmt.Printf("AMAT (With VC)        : %.2f Cycles\n", amatWithVC)
	fmt.Printf("Performance Improv.   : %.2f%%\n", ((amatBase-amatWithVC)/amatBase)*100)
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("Simulation Time       : %.0f cycles\n", float64(engine.CurrentTime()))
	fmt.Println("========================================================")

	s.Terminate()
}


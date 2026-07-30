package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/sarchlab/akita/v5/mem"
	"github.com/sarchlab/akita/v5/benchmarks/trashingmemaccessagent"
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

func buildEnvironment() (*simulation.Simulation, timing.Engine, *trashingmemaccessagent.MemAccessAgent, *writeback.Comp) {
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

	addressToPortMapper := new(mem.SinglePortMapper)
	addressToPortMapper.Port = dram.GetPortByName("Top").AsRemote()

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
			AddressToPortMapper: addressToPortMapper,
		}).
		Build("Cache")
	assignPorts(s, writeBackCache, "Top", "Bottom", "Control")

	agent.LowModule = writeBackCache.GetPortByName("Top")

	conn.PlugIn(agent.GetPortByName("Mem"))
	conn.PlugIn(writeBackCache.GetPortByName("Bottom"))
	conn.PlugIn(writeBackCache.GetPortByName("Top"))
	conn.PlugIn(dram.GetPortByName("Top"))

	return s, engine, agent, writeBackCache
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

	s, engine, agent, cache := buildEnvironment()
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

	fmt.Println("\n=================== CACHE STATISTICS ===================")
    
    hits := cache.State.StatHitCount
    misses := cache.State.StatMissCount
    totalAccesses := hits + misses
	L2ReadTraffic := cache.State.StatL2ReadTraffic
	L2WriteTraffic := cache.State.StatL2WriteTraffic
    
    var hitRate, missRate float64
    if totalAccesses > 0 {
        hitRate = (float64(hits) / float64(totalAccesses)) * 100
        missRate = (float64(misses) / float64(totalAccesses)) * 100
    }

    fmt.Printf("Total Accesses    : %d\n", totalAccesses)
    fmt.Printf("Total Hits        : %d (%.2f%%)\n", hits, hitRate)
    fmt.Printf("Total Misses      : %d (%.2f%%)\n", misses, missRate)
	fmt.Printf("Total L2 Read Traffic    : %d\n", L2ReadTraffic)
	fmt.Printf("Total L2 Write Traffic    : %d\n", L2WriteTraffic)
    fmt.Println("--------------------------------------------------------")
    fmt.Printf("Simulation Time (Cycles) : %.0f cycles\n", float64(engine.CurrentTime()))
    fmt.Println("========================================================")

	s.Terminate()
}

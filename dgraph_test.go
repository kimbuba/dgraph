package dgraph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDot(t *testing.T) {
	subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", "B")
	subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		return nil
	}))
	subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
		return nil
	}))
	subGraph, err := subGraphBuilder.Compile()
	require.Nil(t, err)

	subGraph2Builder := NewSubGraphBuilder("Subgraph2", "A", "B")
	subGraph2Builder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		return nil
	}))
	subGraph2Builder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
		return nil
	}))
	subGraph2, err := subGraph2Builder.Compile()
	require.Nil(t, err)

	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewDynamicNodeImpl(
		"A",
		[]string{"B", "C", "Subgraph", "Subgraph2"},
		func(ctx context.Context, state *State) (string, error) { return "B", nil },
	))
	graphBuilder.AddNode(subGraph)
	graphBuilder.AddNode(subGraph2)
	graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error { return nil }))
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error { return nil }))
	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, graph.ToDOT())
	//print(graph.ToDOT())
}

func TestCompile(t *testing.T) {
	{ // A - (B,C) - C -> END (Valid simple graph)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state *State) error {
			return nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))

		_, err := graphBuilder.Compile()
		require.Nil(t, err)
	}

	{ // A - (B,C) - C -> END (With subgraph and parallel node)
		subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", "C")
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		require.Nil(t, err)

		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"Subgraph", "C"}, func(ctx context.Context, state *State) (string, error) {
			return "Subgraph", nil
		}))
		graphBuilder.AddNode(subGraph)

		nodeCParallel := NewInnerNodeImpl(func(ctx context.Context, state *State) error {
			return nil
		})

		cParallelNode := NewParallelNode("C", EndNode, nodeCParallel, nodeCParallel)

		graphBuilder.AddNode(cParallelNode)

		_, err = graphBuilder.Compile()
		require.Nil(t, err)
	}

	{ // A -> B -> A (Cycle, but C provides path to END)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "A", func(ctx context.Context, state *State) error {
			return nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		_, err := graphBuilder.Compile()
		require.Nil(t, err) // Compile allows cycles if END is reachable
	}

	{ // A -> (B, D), D node not added (Non-existent next node listed)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "D"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state *State) error {
			return nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))

		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [A] lists non-existent node [D] in PossibleNexts")
	}

	{ // A -> B -> C -> END, D -> C (Unreachable node D)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state *State) error {
			return nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("D", "C", func(ctx context.Context, state *State) error { // D is defined but not reachable
			return nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))

		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [D] is unreachable from startPoint [A]")
	}

	{ // A -> B, B -> C, C node not added (Non-existent next node referenced)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state *State) error { // C does not exist
			return nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [B] lists non-existent node [C] in PossibleNexts")
	}

	{ // A -> B -> A (Cycle with no path to END)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state *State) (string, error) {
			return "B", nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "A", func(ctx context.Context, state *State) error {
			return nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, no path from startPoint [A] can reach END")
	}

	{ // StartPoint node 'A' is specified but not added
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		// No node 'A' is added
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, startPoint node [A] not found")
	}

	{ // Minimal valid graph: A -> END
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		_, err := graphBuilder.Compile()
		require.Nil(t, err)
	}

	{ // AddNode(nil)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		require.PanicsWithValue(t, "graphBuilder 'Main': AddNode failed, node cannot be nil", func() {
			graphBuilder.AddNode(nil)
		})
	}

	{ // AddNode with empty name
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		require.PanicsWithValue(t, "graphBuilder 'Main': AddNode failed, node name cannot be empty", func() {
			graphBuilder.AddNode(NewStaticNodeImpl("", EndNode, func(ctx context.Context, state *State) error { return nil }))
		})
	}

	{ // AddNode with reserved name 'END'
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		require.PanicsWithValue(t, "graphBuilder 'Main': AddNode failed, cannot add a node with the reserved name 'END'", func() {
			graphBuilder.AddNode(NewStaticNodeImpl(EndNode, "SOME_OTHER_NODE", func(ctx context.Context, state *State) error { return nil }))
		})
	}

	{ // Add the duplicate node name
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		nodeA := NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state *State) error { return nil })
		graphBuilder.AddNode(nodeA)
		require.PanicsWithValue(t, "graphBuilder 'Main': AddNode failed, node [A] already exists", func() {
			graphBuilder.AddNode(nodeA)
		})
	}

	// Subgraph with no join point
	require.PanicsWithValue(t, "Warning: Creating SubGraphBuilder 'SubgraphNoJoin' with potentially missing name, start point, or parent join point.", func() {
		NewSubGraphBuilder("SubgraphNoJoin", "A", "")
	})

	// ParallelNode with missing arguments
	require.PanicsWithValue(t, "Warning: Creating ParallelNode '' with potentially missing components.", func() {
		NewParallelNode("", "")
	})

	// Node that lists itself as possible next
	{
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"A"}, func(ctx context.Context, state *State) (string, error) {
			return "A", nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, no path from startPoint [A] can reach END")
	}

	// Empty graphbuilder
	{
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, startPoint node [A] not found")
	}

	// Disconnected graph (node B is never reached)
	{
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state *State) error { return nil }))
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error { return nil }))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [B] is unreachable from startPoint [A]")
	}

	// Multiple paths to END, only one END
	{
		subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", EndNode)
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		require.Nil(t, err)

		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C", "Subgraph"}, func(ctx context.Context, state *State) (string, error) { return "B", nil }))
		graphBuilder.AddNode(subGraph)
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error { return nil }))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error { return nil }))
		_, err = graphBuilder.Compile()
		require.Nil(t, err)
	}
}

func TestAToBParallel(t *testing.T) {
	// Parallel Node
	nodeBParallel := NewInnerNodeImpl(func(ctx context.Context, state *State) error {
		state.MU.Lock()
		count, _ := Get[int64](state.Data, "count")
		state.Data["count"] = count + 1
		state.MU.Unlock()
		return nil
	})

	bParallelNode := NewParallelNode("B", "C", nodeBParallel, nodeBParallel)

	// A - B (parallel) - C
	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		state.Data["mainVisitedA"] = true
		return nil
	}))
	graphBuilder.AddNode(bParallelNode)
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
		state.Data["mainVisitedC"] = true
		return nil
	}))

	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := State{
		Data: make(map[any]any),
	}
	state.Data["count"] = 0
	finalResult, err := graph.Run(t.Context(), &state)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State.Data["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State.Data["mainVisitedA"])
	}

	if _, ok := finalResult.State.Data["mainVisitedC"]; !ok {
		t.Errorf("Expected state to mark C as visited, but got: %v", finalResult.State.Data["mainVisitedC"])
	}
	totalCount, _ := Get[int64](finalResult.State.Data, "count")
	require.Equal(t, int64(2), totalCount)
}

func TestAToBDynamicParallel(t *testing.T) {
	// Parallel Node
	nodeBParallel := NewInnerNodeImpl(func(ctx context.Context, state *State) error {
		state.MU.Lock()
		count, _ := Get[int64](state.Data, "count")
		state.Data["count"] = count + 1
		state.MU.Unlock()
		return nil
	})

	bParallelNode := NewDynamicParallelNode("B", "C", func(ctx context.Context, state *State) ([]Node, error) {
		return []Node{nodeBParallel, nodeBParallel}, nil
	})

	// A - B (parallel) - C
	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		state.Data["mainVisitedA"] = true
		return nil
	}))
	graphBuilder.AddNode(bParallelNode)
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
		state.Data["mainVisitedC"] = true
		return nil
	}))

	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := State{
		Data: make(map[any]any),
	}
	state.Data["count"] = 0
	finalResult, err := graph.Run(t.Context(), &state)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State.Data["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State.Data["mainVisitedA"])
	}

	if _, ok := finalResult.State.Data["mainVisitedC"]; !ok {
		t.Errorf("Expected state to mark C as visited, but got: %v", finalResult.State.Data["mainVisitedC"])
	}
	totalCount, _ := Get[int64](finalResult.State.Data, "count")
	require.Equal(t, int64(2), totalCount)
}

func TestAToSubGraphToD(t *testing.T) {
	// A - B - SubGraph (A-B) - C
	subGraphBuilder := NewSubGraphBuilder("SubGraph", "A", "C")
	subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		state.Data["subGraphVisitedA"] = true
		return nil
	}))
	subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
		state.Data["subGraphVisitedB"] = true
		return nil
	}))
	subGraph, err := subGraphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "SubGraph", func(ctx context.Context, state *State) error {
		state.Data["mainVisitedA"] = true
		return nil
	}))
	graphBuilder.AddNode(subGraph)
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
		state.Data["mainVisitedC"] = true
		return nil
	}))

	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := State{
		Data: make(map[any]any),
	}
	finalResult, err := graph.Run(t.Context(), &state)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State.Data["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State.Data["mainVisitedA"])
	}
	if _, ok := finalResult.State.Data["subGraphVisitedA"]; !ok {
		t.Errorf("Expected state to mark subGrap A as visited, but got: %v", finalResult.State.Data["subGraphVisitedA"])
	}
	if _, ok := finalResult.State.Data["subGraphVisitedB"]; !ok {
		t.Errorf("Expected state to mark subGrap B as visited, but got: %v", finalResult.State.Data["subGraphVisitedB"])
	}
	if _, ok := finalResult.State.Data["mainVisitedC"]; !ok {
		t.Errorf("Expected state to mark C as visited, but got: %v", finalResult.State.Data["mainVisitedC"])
	}
}

func TestAToB(t *testing.T) {
	// A - B
	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
		state.Data["mainVisitedA"] = true
		return nil
	}))
	graphBuilder.AddNode(NewDynamicNodeImpl("B", []string{EndNode, "A"}, func(ctx context.Context, state *State) (string, error) {
		state.Data["mainVisitedB"] = true
		return EndNode, nil
	}))
	// Compile the main graph
	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := State{
		Data: make(map[any]any),
	}
	finalResult, err := graph.Run(t.Context(), &state)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State.Data["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State.Data["mainVisitedA"])
	}
	if _, ok := finalResult.State.Data["mainVisitedB"]; !ok {
		t.Errorf("Expected state to mark B as visited, but got: %v", finalResult.State.Data["mainVisitedB"])
	}
}

func TestAToBMaxIterations(t *testing.T) {
	createGraph := func(maxIterations int) *Graph {
		subGraphBuilder := NewSubGraphBuilder("SubGraph", "A", "C")
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		if err != nil {
			t.Fatal(err)
		}

		graphBuilder := NewGraphBuilder("Main", "A", maxIterations)
		graphBuilder.AddNode(NewStaticNodeImpl("A", "SubGraph", func(ctx context.Context, state *State) error {
			return nil
		}))
		graphBuilder.AddNode(subGraph)
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state *State) error {
			return nil
		}))
		// Compile the main graph
		graph, err := graphBuilder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return graph
	}
	{
		state := State{
			Data: make(map[any]any),
		}
		_, err := createGraph(5).Run(t.Context(), &state)
		require.Nil(t, err)
	}
	{
		state := State{
			Data: make(map[any]any),
		}
		_, err := createGraph(4).Run(t.Context(), &state)
		require.EqualError(t, err, "Max iterations exceeded")
	}
}

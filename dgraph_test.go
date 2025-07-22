package main

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

//func TestDot(t *testing.T) {
//	subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", "B")
//	subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
//		return state, nil
//	}))
//	subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
//		return state, nil
//	}))
//	subGraph, err := subGraphBuilder.Compile()
//	require.Nil(t, err)
//
//	subGraph2Builder := NewSubGraphBuilder("Subgraph2", "A", "B")
//	subGraph2Builder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
//		return state, nil
//	}))
//	subGraph2Builder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
//		return state, nil
//	}))
//	subGraph2, err := subGraph2Builder.Compile()
//	require.Nil(t, err)
//
//	graphBuilder := NewGraphBuilder("Main", "A", 100)
//	graphBuilder.AddNode(NewDynamicNodeImpl(
//		"A",
//		[]string{"B", "C", "Subgraph", "Subgraph2"},
//		func(ctx context.Context, state State) (string, State, error) { return "B", state, nil },
//	))
//	graphBuilder.AddNode(subGraph)
//	graphBuilder.AddNode(subGraph2)
//	graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
//	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
//	graph, err := graphBuilder.Compile()
//	if err != nil {
//		t.Fatal(err)
//	}
//	print(graph.ToDOT())
//}

func TestCompile(t *testing.T) {
	{ // A - (B,C) - C -> END (Valid simple graph)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))

		_, err := graphBuilder.Compile()
		require.Nil(t, err)
	}

	{ // A - (B,C) - C -> END (With subgraph and parallel node)
		subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", "C")
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		require.Nil(t, err)

		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"Subgraph", "C"}, func(ctx context.Context, state State) (string, State, error) {
			return "Subgraph", state, nil
		}))
		graphBuilder.AddNode(subGraph)

		nodeCParallel := NewInnerNodeImpl(func(ctx context.Context, state State) (State, error) {
			return state, nil
		})

		cParallelNode := NewParallelNode("C", nodeCParallel, func(initialState State) ([]State, error) {
			states := lo.Times(2, func(i int) State {
				return make(State)
			})
			return states, nil
		}, func(initialState State, branchResults []State) (State, error) {
			return initialState, nil
		}, EndNode)

		graphBuilder.AddNode(cParallelNode)

		_, err = graphBuilder.Compile()
		require.Nil(t, err)
	}

	{ // A -> B -> A (Cycle, but C provides path to END)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "A", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		_, err := graphBuilder.Compile()
		require.Nil(t, err) // Compile allows cycles if END is reachable
	}

	{ // A -> (B, D), D node not added (Non-existent next node listed)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "D"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))

		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [A] lists non-existent node [D] in PossibleNexts")
	}

	{ // A -> B -> C -> END, D -> C (Unreachable node D)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("D", "C", func(ctx context.Context, state State) (State, error) { // D is defined but not reachable
			return state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))

		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [D] is unreachable from startPoint [A]")
	}

	{ // A -> B, B -> C, C node not added (Non-existent next node referenced)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "C", func(ctx context.Context, state State) (State, error) { // C does not exist
			return state, nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [B] lists non-existent node [C] in PossibleNexts")
	}

	{ // A -> B -> A (Cycle with no path to END)
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B"}, func(ctx context.Context, state State) (string, State, error) {
			return "B", state, nil
		}))
		graphBuilder.AddNode(NewStaticNodeImpl("B", "A", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, no path from startPoint [A] can reach END")
	}

	{ // StartPoint node 'A' is specified but not added
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		// No node 'A' is added
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, startPoint node [A] not found")
	}

	{ // Minimal valid graph: A -> END
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
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
			graphBuilder.AddNode(NewStaticNodeImpl("", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
		})
	}

	{ // AddNode with reserved name 'END'
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		require.PanicsWithValue(t, "graphBuilder 'Main': AddNode failed, cannot add a node with the reserved name 'END'", func() {
			graphBuilder.AddNode(NewStaticNodeImpl(EndNode, "SOME_OTHER_NODE", func(ctx context.Context, state State) (State, error) { return state, nil }))
		})
	}

	{ // Add the duplicate node name
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		nodeA := NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil })
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
		NewParallelNode("", nil, nil, nil, "")
	})

	// Node that lists itself as possible next
	{
		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"A"}, func(ctx context.Context, state State) (string, State, error) {
			return "A", state, nil
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
		graphBuilder.AddNode(NewStaticNodeImpl("A", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
		_, err := graphBuilder.Compile()
		require.EqualError(t, err, "graphBuilder 'Main': Compile failed, node [B] is unreachable from startPoint [A]")
	}

	// Multiple paths to END, only one END
	{
		subGraphBuilder := NewSubGraphBuilder("Subgraph", "A", EndNode)
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		require.Nil(t, err)

		graphBuilder := NewGraphBuilder("Main", "A", 100)
		graphBuilder.AddNode(NewDynamicNodeImpl("A", []string{"B", "C", "Subgraph"}, func(ctx context.Context, state State) (string, State, error) { return "B", state, nil }))
		graphBuilder.AddNode(subGraph)
		graphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) { return state, nil }))
		_, err = graphBuilder.Compile()
		require.Nil(t, err)
	}
}

func TestAToBParallel(t *testing.T) {
	// Parallel Node
	nodeBParallel := NewInnerNodeImpl(func(ctx context.Context, state State) (State, error) {
		state["count"] = 1
		return state, nil
	})

	bParallelNode := NewParallelNode("B", nodeBParallel, func(initialState State) ([]State, error) {
		states := lo.Times(2, func(i int) State {
			return make(State)
		})
		return states, nil
	}, func(initialState State, branchResults []State) (State, error) {
		total := lo.Reduce(branchResults, func(acc int, item State, _ int) int {
			if count, ok := item["count"].(int); ok {
				return acc + count
			}
			return acc
		}, 0)
		initialState["total"] = total
		return initialState, nil
	}, "C")

	// A - B (parallel) - C
	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
		state["mainVisitedA"] = true
		return state, nil
	}))
	graphBuilder.AddNode(bParallelNode)
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
		state["mainVisitedC"] = true
		return state, nil
	}))

	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	finalResult, err := graph.Run(t.Context(), State{})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State["mainVisitedA"])
	}

	if count, ok := finalResult.State["total"].(int); !ok || count != 2 {
		t.Errorf("Expected state to mark parallel B as visited twice , but got: %v", finalResult.State["subGraphVisitedA"])
	}
	if _, ok := finalResult.State["mainVisitedC"]; !ok {
		t.Errorf("Expected state to mark C as visited, but got: %v", finalResult.State["mainVisitedC"])
	}
}

func TestAToSubGraphToD(t *testing.T) {
	// A - B - SubGraph (A-B) - C
	subGraphBuilder := NewSubGraphBuilder("SubGraph", "A", "C")
	subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
		state["subGraphVisitedA"] = true
		return state, nil
	}))
	subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
		state["subGraphVisitedB"] = true
		return state, nil
	}))
	subGraph, err := subGraphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "SubGraph", func(ctx context.Context, state State) (State, error) {
		state["mainVisitedA"] = true
		return state, nil
	}))
	graphBuilder.AddNode(subGraph)
	graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
		state["mainVisitedC"] = true
		return state, nil
	}))

	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	finalResult, err := graph.Run(t.Context(), State{})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State["mainVisitedA"])
	}
	if _, ok := finalResult.State["subGraphVisitedA"]; !ok {
		t.Errorf("Expected state to mark subGrap A as visited, but got: %v", finalResult.State["subGraphVisitedA"])
	}
	if _, ok := finalResult.State["subGraphVisitedB"]; !ok {
		t.Errorf("Expected state to mark subGrap B as visited, but got: %v", finalResult.State["subGraphVisitedB"])
	}
	if _, ok := finalResult.State["mainVisitedC"]; !ok {
		t.Errorf("Expected state to mark C as visited, but got: %v", finalResult.State["mainVisitedC"])
	}
}

func TestAToB(t *testing.T) {
	// A - B
	graphBuilder := NewGraphBuilder("Main", "A", 100)
	graphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
		state["mainVisitedA"] = true
		return state, nil
	}))
	graphBuilder.AddNode(NewDynamicNodeImpl("B", []string{EndNode, "A"}, func(ctx context.Context, state State) (string, State, error) {
		state["mainVisitedB"] = true
		return EndNode, state, nil
	}))
	// Compile the main graph
	graph, err := graphBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err := graph.Run(t.Context(), State{})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := finalResult.State["mainVisitedA"]; !ok {
		t.Errorf("Expected state to mark A as visited, but got: %v", finalResult.State["mainVisitedA"])
	}
	if _, ok := finalResult.State["mainVisitedB"]; !ok {
		t.Errorf("Expected state to mark B as visited, but got: %v", finalResult.State["mainVisitedB"])
	}
}

func TestAToBMaxIterations(t *testing.T) {
	createGraph := func(maxIterations int) *Graph {
		subGraphBuilder := NewSubGraphBuilder("SubGraph", "A", "C")
		subGraphBuilder.AddNode(NewStaticNodeImpl("A", "B", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraphBuilder.AddNode(NewStaticNodeImpl("B", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		subGraph, err := subGraphBuilder.Compile()
		if err != nil {
			t.Fatal(err)
		}

		graphBuilder := NewGraphBuilder("Main", "A", maxIterations)
		graphBuilder.AddNode(NewStaticNodeImpl("A", "SubGraph", func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		graphBuilder.AddNode(subGraph)
		graphBuilder.AddNode(NewStaticNodeImpl("C", EndNode, func(ctx context.Context, state State) (State, error) {
			return state, nil
		}))
		// Compile the main graph
		graph, err := graphBuilder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return graph
	}

	{
		_, err := createGraph(5).Run(t.Context(), State{})
		require.Nil(t, err)
	}
	{
		_, err := createGraph(4).Run(t.Context(), State{})
		require.EqualError(t, err, "Max iterations exceeded")
	}
}

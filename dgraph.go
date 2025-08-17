package dgraph

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/go-errors/errors"
	"golang.org/x/sync/errgroup"
)

var ErrMaxIterations = errors.New("Max iterations exceeded")

const EndNode = "END"

type State struct {
	MU   sync.RWMutex
	Data map[any]any
}

type NodeResult struct {
	Next       string
	State      *State
	Iterations int
}

type Node interface {
	Name() string
	PossibleNexts() []string
	Run(ctx context.Context, state *State) (NodeResult, error)
}

type NodeImpl struct {
	name           string
	staticNext     string
	possibleNexts  []string
	staticHandler  func(context.Context, *State) error
	dynamicHandler func(context.Context, *State) (string, error)
}

func NewInnerNodeImpl(handler func(context.Context, *State) error) *NodeImpl {
	return &NodeImpl{staticHandler: handler, possibleNexts: []string{}}
}

func NewStaticNodeImpl(name string, nextNode string, handler func(context.Context, *State) error) *NodeImpl {
	return &NodeImpl{name: name, staticNext: nextNode, staticHandler: handler, possibleNexts: []string{nextNode}}
}

func NewDynamicNodeImpl(name string, possibleNexts []string, handler func(context.Context, *State) (string, error)) *NodeImpl {
	return &NodeImpl{name: name, possibleNexts: possibleNexts, dynamicHandler: handler}
}

func (n *NodeImpl) Name() string {
	return n.name
}

func (n *NodeImpl) PossibleNexts() []string {
	return n.possibleNexts
}

func (n *NodeImpl) Run(ctx context.Context, s *State) (NodeResult, error) {
	if n.dynamicHandler != nil {
		return n.doRunDynamic(ctx, s)
	}
	return n.doRunStatic(ctx, s)
}

func (n *NodeImpl) doRunStatic(ctx context.Context, s *State) (NodeResult, error) {
	if err := n.staticHandler(ctx, s); err != nil {
		return NodeResult{}, err
	}
	return NodeResult{Next: n.staticNext, State: s, Iterations: 1}, nil
}

func (n *NodeImpl) doRunDynamic(ctx context.Context, s *State) (NodeResult, error) {
	next, err := n.dynamicHandler(ctx, s)
	return NodeResult{Next: next, State: s, Iterations: 1}, err
}

type ParallelNode struct {
	name       string
	innerNodes []Node
	next       string
}

func NewParallelNode(
	name string,
	next string, // Name of the next node in the graph after this parallel node completes
	innerNodes ...Node, // The nodes instance to run in each branch
) *ParallelNode {
	if name == "" || innerNodes == nil || next == "" {
		panic(fmt.Sprintf("Warning: Creating ParallelNode '%s' with potentially missing components.", name))
	}
	return &ParallelNode{
		name:       name,
		innerNodes: innerNodes,
		next:       next,
	}
}

func (p *ParallelNode) Name() string {
	return p.name
}
func (p *ParallelNode) PossibleNexts() []string {
	return []string{p.next}
}

func (p *ParallelNode) Run(ctx context.Context, state *State) (NodeResult, error) {
	nodeName := p.Name()
	return runInnerNodes(ctx, state, nodeName, p.next, p.innerNodes)
}

type DynamicParallelNode struct {
	name         string
	nodeProvider func(context.Context, *State) ([]Node, error)
	next         string
}

func NewDynamicParallelNode(
	name string,
	next string, // Name of the next node in the graph after this parallel node completes
	nodeProvider func(context.Context, *State) ([]Node, error), // Function that returns nodes to run in parallel
) *DynamicParallelNode {
	if name == "" || nodeProvider == nil || next == "" {
		panic(fmt.Sprintf("Warning: Creating DynamicParallelNode '%s' with potentially missing components.", name))
	}
	return &DynamicParallelNode{
		name:         name,
		nodeProvider: nodeProvider,
		next:         next,
	}
}

func (dp *DynamicParallelNode) Name() string {
	return dp.name
}

func (dp *DynamicParallelNode) PossibleNexts() []string {
	return []string{dp.next}
}

func (dp *DynamicParallelNode) Run(ctx context.Context, state *State) (NodeResult, error) {
	nodeName := dp.Name()
	// Get the dynamic list of nodes to run in parallel
	innerNodes, err := dp.nodeProvider(ctx, state)
	if err != nil {
		return NodeResult{}, errors.WrapPrefix(err, fmt.Sprintf("dynamic parallel node '%s': error getting nodes from provider", nodeName), 0)
	}
	return runInnerNodes(ctx, state, nodeName, dp.next, innerNodes)
}

func runInnerNodes(ctx context.Context, state *State, nodeName string, next string, innerNodes []Node) (NodeResult, error) {
	numBranches := len(innerNodes)
	if numBranches == 0 {
		return NodeResult{}, errors.Errorf("Parallel node '%s': split resulted in 0 branches", nodeName)
	}

	// fmt.Printf("Parallel node '%s': Starting %d branches...\n", nodeName, numBranches)

	var eg *errgroup.Group
	eg, childCtx := errgroup.WithContext(ctx)

	for i := 0; i < numBranches; i++ {
		index := i
		node := innerNodes[index]

		eg.Go(func() error {
			recoverGoRoutine()
			result, err := node.Run(childCtx, state)
			if err != nil {
				return errors.WrapPrefix(err, fmt.Sprintf("branch %d (inner node '%s')", index, node.Name()), 0)
			}
			if result.Next != "" {
				return errors.Errorf("Inner node Next must be empty it, was '%s'", result.Next)
			}
			return nil
		})
	}

	// Wait for all branches to complete and check for errors
	if err := eg.Wait(); err != nil {
		return NodeResult{}, errors.WrapPrefix(err, fmt.Sprintf("parallel node '%s': error during parallel execution", nodeName), 0)
	}

	return NodeResult{Next: next, State: state, Iterations: 1}, nil
}

// Graph represents a compiled, immutable computation graph.
type Graph struct {
	name              string
	startPoint        string
	subGraphJoinPoint string
	maxIterations     int
	nodes             map[string]Node
}

// Name returns the name of the graph.
func (g *Graph) Name() string {
	return g.name
}

// PossibleNexts returns the node the graph transitions to upon completion.
// For a subgraph, this is its join point in the parent graph.
// For the main graph, this might conceptually be an external "Finish" or empty if not embedded.
func (g *Graph) PossibleNexts() []string {
	// If it's a subgraph with a defined join point, return that.
	if g.subGraphJoinPoint != "" {
		return []string{g.subGraphJoinPoint}
	}
	// Otherwise (likely the main graph or misconfigured subgraph), it has no defined 'next' in this context.
	// Or, if the main graph should always end, return []string{EndNode}?
	// Let's assume for DOT generation and structure, the join point matters most.
	// If it needs to connect to EndNode implicitly, that's handled in DOT generation.
	return []string{} // An empty string indicates it's the final step conceptually *or* connects to the implicit EndNode
}

// Run executes the graph starting from its entry point.
//
//nolint:gocognit
func (g *Graph) Run(ctx context.Context, state *State) (NodeResult, error) {
	currentNode, ok := g.nodes[g.startPoint]
	if !ok {
		// This should ideally be caught by Compile, but double-check.
		return NodeResult{}, errors.Errorf("graph '%s': run failed, entry node [%s] not found", g.name, g.startPoint)
	}
	iterations := 0
	for {
		select {
		case <-ctx.Done():
			return NodeResult{}, errors.WrapPrefix(ctx.Err(), fmt.Sprintf("graph '%s': context cancelled while running node '%s'", g.name, currentNode.Name()), 0)
		default:
			// Continue execution
		}

		result, err := currentNode.Run(ctx, state)
		if err != nil {
			return NodeResult{}, errors.WrapPrefix(err, fmt.Sprintf("graph '%s': error running node '%s'", g.name, currentNode.Name()), 0)
		}
		iterations += result.Iterations
		if g.maxIterations > 0 && iterations >= g.maxIterations {
			return NodeResult{}, ErrMaxIterations
		}

		// Check next was a valid node between all possible nexts.
		if result.Next != EndNode {
			possibleNexts := currentNode.PossibleNexts()
			isValidNext := false
			for _, possible := range possibleNexts {
				if result.Next == possible {
					isValidNext = true
					break
				}
			}
			if !isValidNext {
				return NodeResult{}, errors.Errorf("graph '%s': runtime error - node [%s] returned invalid next node [%s]. Allowed: %v",
					g.name, currentNode.Name(), result.Next, possibleNexts)
			}
		}

		state = result.State
		if result.Next == EndNode {
			// Set Next to g.subGraphEndPoint for sub-graphs.
			return NodeResult{Next: g.subGraphJoinPoint, State: state, Iterations: iterations}, nil
		}
		nextNode, ok := g.nodes[result.Next]
		if !ok {
			// This check is crucial at runtime if Compile doesn't validate all paths.
			return NodeResult{}, errors.Errorf("graph '%s': run failed, next node [%s] requested by node [%s] not found", g.name, result.Next, currentNode.Name())
		}
		currentNode = nextNode
	}
}

// GraphBuilder is used to construct and validate a Graph.
type GraphBuilder struct {
	name              string
	startPoint        string
	subGraphJoinPoint string
	maxIterations     int
	nodes             map[string]Node
}

func NewSubGraphBuilder(name string, subGraphStartPoint string, parentGraphJoinPoint string) *GraphBuilder {
	if name == "" || subGraphStartPoint == "" || parentGraphJoinPoint == "" {
		panic(fmt.Sprintf("Warning: Creating SubGraphBuilder '%s' with potentially missing name, start point, or parent join point.", name))
	}
	return &GraphBuilder{
		name:              name,
		startPoint:        subGraphStartPoint,
		subGraphJoinPoint: parentGraphJoinPoint,
		nodes:             make(map[string]Node),
	}
}

func NewGraphBuilder(name string, startPoint string, maxIterations int) *GraphBuilder {
	if name == "" || startPoint == "" {
		panic(fmt.Sprintf("Warning: Creating GraphBuilder '%s' with potentially missing name or start point.", name))
	}
	return &GraphBuilder{
		name:          name,
		startPoint:    startPoint,
		maxIterations: maxIterations,
		nodes:         make(map[string]Node),
	}
}

// AddNode adds a node to the graph being built.
func (gb *GraphBuilder) AddNode(node Node) {
	if node == nil {
		panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, node cannot be nil", gb.name))
	}
	name := node.Name()
	if name == "" {
		panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, node name cannot be empty", gb.name))
	}
	if name == EndNode {
		panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, cannot add a node with the reserved name '%s'", gb.name, EndNode))
	}
	if _, exists := gb.nodes[name]; exists {
		panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, node [%s] already exists", gb.name, name))
	}
	// If the node being added is itself a Graph (subgraph), perform basic validation.
	if subGraph, ok := node.(*Graph); ok {
		if subGraph.startPoint == "" {
			panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, subgraph [%s] has no startPoint defined", gb.name, subGraph.name))
		}
		if subGraph.subGraphJoinPoint == "" {
			// The subgraph itself MUST define where the parent graph should go after it finishes.
			panic(fmt.Sprintf("graphBuilder '%s': AddNode failed, subgraph [%s] has no subGraphJoinPoint defined", gb.name, subGraph.name))
		}
	}
	gb.nodes[name] = node
}

// Compile validates the graph structure and returns an immutable Graph
// or an error if validation fails.
// Validation checks:
// 1. StartPoint node exists.
// 2. All nodes listed in PossibleNexts exist (or are EndNode).
// 3. All nodes added to the builder are reachable from the StartPoint.
// 4. It is possible to reach EndNode from the StartPoint.
//
//nolint:gocognit
func (gb *GraphBuilder) Compile() (*Graph, error) {
	// 1. Check Start Point Exists
	if _, ok := gb.nodes[gb.startPoint]; !ok {
		return nil, errors.Errorf("graphBuilder '%s': Compile failed, startPoint node [%s] not found", gb.name, gb.startPoint)
	}

	// 2. Check all PossibleNexts exist for every node
	for name, node := range gb.nodes {
		for _, nextName := range node.PossibleNexts() {
			if nextName == EndNode {
				continue // EndNode is implicitly valid
			}
			if _, exists := gb.nodes[nextName]; !exists {
				return nil, errors.Errorf("graphBuilder '%s': Compile failed, node [%s] lists non-existent node [%s] in PossibleNexts", gb.name, name, nextName)
			}
		}
		// If node is a subgraph, perform basic internal check
		if subGraph, isSub := node.(*Graph); isSub {
			if _, subStartExists := subGraph.nodes[subGraph.startPoint]; !subStartExists {
				//nolint:revive
				return nil, errors.Errorf("graphBuilder '%s': Compile failed, subgraph [%s] referenced in node [%s] has an invalid internal startPoint [%s]", gb.name, subGraph.Name(), name, subGraph.startPoint)
			}
		}
	}

	// 3. Check Reachability: All nodes must be reachable from startPoint
	reachableNodes := gb.findReachableNodes() // Uses BFS/DFS internally, explores all branches
	for name := range gb.nodes {
		if _, isReachable := reachableNodes[name]; !isReachable {
			return nil, errors.Errorf("graphBuilder '%s': Compile failed, node [%s] is unreachable from startPoint [%s]", gb.name, name, gb.startPoint)
		}
	}

	// 4. Check END Reachability: Must be possible to reach END from startPoint
	endIsReachable, err := gb.isEndReachableFromStart()
	if err != nil {
		// This might indicate internal errors or inconsistencies caught by the check
		return nil, errors.WrapPrefix(err, fmt.Sprintf("graphBuilder '%s': Compile failed during END reachability check", gb.name), 0)
	}
	if !endIsReachable {
		return nil, errors.Errorf("graphBuilder '%s': Compile failed, no path from startPoint [%s] can reach END", gb.name, gb.startPoint)
	}

	// If all checks pass, create the immutable Graph
	compiledNodes := make(map[string]Node, len(gb.nodes))
	for k, v := range gb.nodes {
		// Consider deep copying nodes if they are mutable and need protection
		compiledNodes[k] = v
	}

	return &Graph{
		name:              gb.name,
		startPoint:        gb.startPoint,
		subGraphJoinPoint: gb.subGraphJoinPoint, // Will be "" for main graphs
		maxIterations:     gb.maxIterations,
		nodes:             compiledNodes,
	}, nil
}

// findReachableNodes performs a graph traversal (BFS) from the startPoint
// to find all reachable node names within this builder's context.
// It correctly explores all PossibleNexts for each node.
func (gb *GraphBuilder) findReachableNodes() map[string]struct{} {
	reachable := make(map[string]struct{})
	if _, exists := gb.nodes[gb.startPoint]; !exists {
		return reachable // Start node doesn't even exist in the builder
	}

	queue := []string{gb.startPoint}
	visited := make(map[string]bool) // Tracks nodes added to the queue/visited

	visited[gb.startPoint] = true
	reachable[gb.startPoint] = struct{}{}

	for len(queue) > 0 {
		currentName := queue[0]
		queue = queue[1:]

		currentNode, exists := gb.nodes[currentName]
		if !exists {
			// This node name came from PossibleNexts but wasn't added via AddNode.
			// Should have been caught by the PossibleNexts check in Compile, but defensive.
			continue
		}

		// Explore all possible next steps from the current node
		for _, nextName := range currentNode.PossibleNexts() {
			if nextName == EndNode {
				continue // Don't add EndNode to the queue
			}

			// Check if the next node *exists* in the builder's definition
			if _, nextExists := gb.nodes[nextName]; nextExists {
				if !visited[nextName] {
					visited[nextName] = true
					reachable[nextName] = struct{}{}
					queue = append(queue, nextName)
				}
			}
			// If nextName doesn't exist in gb.nodes, the Compile validation step (2) should have caught it.
		}

		// If the current node is a subgraph, we treat it as a single node at this level.
		// Its internal reachability is checked when the subgraph itself is compiled.
		// The PossibleNexts() for a subgraph node correctly points to its join point in *this* graph.
	}
	return reachable
}

// isEndReachableFromStart checks if there is at least one path from the startPoint
// to the conceptual EndNode using BFS. Handles cycles.
func (gb *GraphBuilder) isEndReachableFromStart() (bool, error) {
	if _, exists := gb.nodes[gb.startPoint]; !exists {
		// Should be caught by Compile's first check
		return false, errors.Errorf("startPoint [%s] not found in graph builder", gb.startPoint)
	}

	queue := []string{gb.startPoint}
	visited := make(map[string]bool) // Nodes added to the queue

	visited[gb.startPoint] = true

	for len(queue) > 0 {
		currentName := queue[0]
		queue = queue[1:]

		currentNode, exists := gb.nodes[currentName]
		if !exists {
			// Should be caught by earlier checks
			continue
		}

		for _, nextName := range currentNode.PossibleNexts() {
			if nextName == EndNode {
				return true, nil // Found a path to END
			}

			// Check if the next node exists in the graph definition
			_, nextExists := gb.nodes[nextName]
			if !nextExists {
				// This indicates an invalid graph structure, should be caught by Compile check 2.
				// Returning error here might be redundant but safer.
				return false, errors.Errorf("node [%s] refers to non-existent next node [%s]", currentName, nextName)
			}

			// Check if we've already processed this node to avoid infinite loops in cycles
			if !visited[nextName] {
				visited[nextName] = true
				queue = append(queue, nextName)
			}
		}
	}

	// If the loop finishes without returning true, END was not reached from any path
	return false, nil
}

// ToDOT returns a visualization of the graph in DOT language, using clusters for subgraphs,
// labeling parallel and dynamic nodes, and supporting overlapping names between graphs/subgraphs.
func (g *Graph) ToDOT() string {
	var b strings.Builder
	b.WriteString("digraph G {\n")
	writeGraphDOT(&b, g, "", map[*Graph]struct{}{})
	b.WriteString("}\n")
	return b.String()
}

// writeGraphDOT writes the graph/subgraph representation into a buffer in DOT language.
// parentPrefix is used to uniquely identify node names belonging to subgraphs/clusters.
//
//nolint:gocognit, funlen
func writeGraphDOT(b *strings.Builder, gr *Graph, parentPrefix string, visited map[*Graph]struct{}) {
	if _, ok := visited[gr]; ok {
		return
	}
	visited[gr] = struct{}{}

	prefix := parentPrefix
	if prefix != "" {
		prefix += "_"
	}

	isCluster := parentPrefix != ""

	if isCluster {
		_, _ = fmt.Fprintf(b, "  subgraph cluster_%s {\n", prefix+gr.name)
		_, _ = fmt.Fprintf(b, "    label=\"subgraph: %s\";\n", gr.name)
	}

	// Create unique node IDs for this subgraph scope
	nodeID := func(name string) string {
		return fmt.Sprintf("\"%s%s\"", prefix, name)
	}
	// For subgraph entry node unique ID (to support edge referencing inside clusters)
	subgraphEntryID := func(nodeName, entry string) string {
		return fmt.Sprintf("\"%s%s_%s\"", prefix, nodeName, entry)
	}

	for nodeName, node := range gr.nodes {
		var nodeLabel string
		nodeType := reflect.TypeOf(node)
		nodeVal := reflect.ValueOf(node)

		// Subgraph node: draw nothing here (no intermediate/invisible node)
		if maybeSub, ok := node.(*Graph); ok {
			// Recurse subgraph with updated prefix
			writeGraphDOT(b, maybeSub, prefix+nodeName, visited)
			continue
		}

		nodeLabel = nodeName
		if nodeType != nil && nodeType.Kind() == reflect.Ptr && nodeType.Elem().Name() == "ParallelNode" {
			nodeLabel += " (parallel)"
		}
		implType := reflect.TypeOf(&NodeImpl{})
		isNodeImpl := false
		if nodeType != nil && nodeType.AssignableTo(implType) {
			isNodeImpl = true
		}
		if nodeType != nil && nodeType.Kind() == reflect.Ptr && nodeType.Elem().Name() == "NodeImpl" {
			isNodeImpl = true
		}
		if isNodeImpl {
			field := nodeVal.Elem().FieldByName("dynamicHandler")
			if field.IsValid() && !field.IsNil() {
				nodeLabel += " (dynamic)"
			}
		}

		_, _ = fmt.Fprintf(b, "    %s [label=\"%s\"];\n", nodeID(nodeName), nodeLabel)
	}

	// Write all edges (including to subgraph entries directly)
	for nodeName, node := range gr.nodes {
		if _, isSubgraph := node.(*Graph); isSubgraph {
			exitNode := node.PossibleNexts()[0]
			_, _ = fmt.Fprintf(b, "    %s_END -> %s;\n", node.Name(), exitNode)
			continue
		}
		for _, next := range node.PossibleNexts() {
			if next == "" {
				continue
			}
			if nextGraph, ok := gr.nodes[next].(*Graph); ok {
				// Edge directly to the entry node of the subgraph, using the fully qualified name
				_, _ = fmt.Fprintf(b, "    %s -> %s;\n", nodeID(nodeName), subgraphEntryID(next, nextGraph.startPoint))
			} else {
				_, _ = fmt.Fprintf(b, "    %s -> %s;\n", nodeID(nodeName), nodeID(next))
			}
		}
	}

	// Draw the real nodes representing subgraph entries for inbound reference
	for nodeName, node := range gr.nodes {
		if subg, ok := node.(*Graph); ok {
			entryNodeID := fmt.Sprintf("\"%s%s_%s\"", prefix, nodeName, subg.startPoint)
			entryLabel := subg.startPoint
			_, _ = fmt.Fprintf(b, "    %s [label=\"%s\"];\n", entryNodeID, entryLabel)
		}
	}

	if isCluster {
		b.WriteString("  }\n")
	}
}

func recoverGoRoutine() {
	if r := recover(); r != nil {
		stack := debug.Stack()
		fmt.Printf("GoRoutinePanicked: [%s] [%s]", r, string(stack))
	}
}

func Get[T any](m map[any]any, key any) (T, bool) {
	// First, check if the key exists in the map.
	val, ok := m[key]
	if !ok {
		// If the key doesn't exist, we can't proceed.
		// We return the zero value of type T (e.g., "" for string, 0 for int).
		var zero T
		return zero, false
	}

	// Now, safely assert the type.
	// This will not panic. If `val` is not of type `T`,
	// `typedVal` will be the zero value of T and `ok` will be false.
	typedVal, ok := val.(T)
	return typedVal, ok
}

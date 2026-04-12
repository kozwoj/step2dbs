package cli

import "strings"

type Node struct {
	Name     string
	Children []*Node
}

func DrawTree(n *Node) string {
	var b strings.Builder
	// Draw root without branch
	b.WriteString(n.Name)
	b.WriteByte('\n')
	// Draw root's children with branches
	for i, c := range n.Children {
		last := i == len(n.Children)-1
		drawNode(&b, c, "", last)
	}
	return b.String()
}

func drawNode(b *strings.Builder, n *Node, prefix string, isLast bool) {
	// Determine branch character
	branch := "├── "
	if isLast {
		branch = "└── "
	}

	// Write the node with prefix and branch
	b.WriteString(prefix)
	b.WriteString(branch)
	b.WriteString(n.Name)
	b.WriteByte('\n')

	// Compute next prefix for children
	var childPrefix string
	if isLast {
		childPrefix = prefix + "    "
	} else {
		childPrefix = prefix + "│   "
	}

	// Render children
	for i, c := range n.Children {
		last := i == len(n.Children)-1
		drawNode(b, c, childPrefix, last)
	}
}

/* example tree printing usage:

var root *Node = &Node{Name: "project"}
root.Children = []*Node{
    {
        Name: "cmd",
        Children: []*Node{
            {Name: "server"},
        },
    },
    {
        Name: "internal",
        Children: []*Node{
            {Name: "index"},
            {Name: "storage"},
        },
    },
    {Name: "go.mod"},
}

fmt.Println(DrawTree(root))

*/

package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"hypernode/util"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
)

// 100MB
const maxBodySize = 100 << 20

type UFMInterface struct {
	Description     string `json:"description"`
	Tier            int    `json:"tier"`
	SystemName      string `json:"system_name"`
	NodeDescription string `json:"node_description"`
	PeerNodeName    string `json:"peer_node_name"`
}

// LeafSwitch represents each single leaf switch.
type LeafSwitch struct {
	Name string
	Tier int
	// NodeNames the real computer node name list in a single leaf switch.
	NodeNames sets.Set[string]
}

// LeafSwitchesGroup represents a group of leaf switches which are connected.
type LeafSwitchesGroup struct {
	Leafs map[string]LeafSwitch
	// NodeList the real computer node name list in a grouped leaf switches.
	NodeNames sets.Set[string]
}

func getLeafSwitches(ufmData []UFMInterface) []LeafSwitch {
	leafMap := make(map[string]*LeafSwitch)

	for _, data := range ufmData {
		// Only need to parse computer ufm data because it containers both leaf and computer connection information.
		if !strings.Contains(data.Description, "Computer") {
			continue
		}
		leafName := data.PeerNodeName
		nodeName := data.SystemName

		if _, exists := leafMap[leafName]; !exists {
			leafMap[leafName] = &LeafSwitch{
				Name:      leafName,
				Tier:      data.Tier,
				NodeNames: sets.New[string](nodeName),
			}
		} else {
			leafMap[leafName].NodeNames.Insert(nodeName)
		}
	}

	result := make([]LeafSwitch, 0, len(leafMap))
	for _, leafSwitch := range leafMap {
		result = append(result, *leafSwitch)
	}

	return result
}

// classifyLeafs classifies leaf switches into groups,
// groupID -> LeafSwitchesGroup
func classifyLeafs(leafSwitches []LeafSwitch) map[string]LeafSwitchesGroup {
	leafMap := make(map[string]LeafSwitch)
	for _, leaf := range leafSwitches {
		leafMap[leaf.Name] = leaf
	}

	leafToNodes := make(map[string]sets.Set[string])
	for _, leaf := range leafSwitches {
		leafToNodes[leaf.Name] = leaf.NodeNames
	}

	nodesToLeafs := make(map[string]sets.Set[string])
	for leaf, nodes := range leafToNodes {
		for node := range nodes {
			if _, exist := nodesToLeafs[node]; !exist {
				nodesToLeafs[node] = sets.New[string]()
			}
			nodesToLeafs[node].Insert(leaf)
		}
	}

	leafGroups := make(map[string]LeafSwitchesGroup)
	groupID := 0
	processedLeafs := sets.New[string]()

	for leaf := range leafToNodes {
		fmt.Printf("process leaf: %s\n", leaf)
		if processedLeafs.Has(leaf) {
			continue
		}

		currentGroupID := fmt.Sprintf("group%d", groupID)
		groupID++

		// Use BFS to Find All Connected Leaf Nodes
		queue := []string{leaf}
		leafGroup := LeafSwitchesGroup{
			Leafs:     make(map[string]LeafSwitch),
			NodeNames: sets.New[string](),
		}

		for len(queue) > 0 {
			currentLeaf := queue[0]
			queue = queue[1:]

			if processedLeafs.Has(currentLeaf) {
				continue
			}

			leafGroup.Leafs[currentLeaf] = leafMap[currentLeaf]
			leafGroup.NodeNames.Insert(leafMap[currentLeaf].NodeNames.UnsortedList()...)
			processedLeafs.Insert(currentLeaf)

			// Find all other leaf nodes that have common nodes with the current leaf node
			for node := range leafToNodes[currentLeaf] {
				for relatedLeaf := range nodesToLeafs[node] {
					if !processedLeafs.Has(relatedLeaf) {
						queue = append(queue, relatedLeaf)
					}
				}
			}
		}

		leafGroups[currentGroupID] = leafGroup
	}

	return leafGroups
}

func LeafSwitchesGroups(ufmData []UFMInterface) map[string]LeafSwitchesGroup {
	leafSwitches := getLeafSwitches(ufmData)
	leafSwitchesGroups := classifyLeafs(leafSwitches)
	return leafSwitchesGroups
}

func main() {
	var kubeconfig string
	var ufmAddr string
	var username string
	var password string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path")
	flag.StringVar(&ufmAddr, "ufm-addr", "", "ufm server address")
	flag.StringVar(&username, "username", "", "ufm user name")
	flag.StringVar(&password, "password", "", "ufn password")

	flag.Parse()

	if kubeconfig == "" {
		kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}
	vcClient, err := util.InitClient(kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to init kube client: %v", err)
	}

	if ufmAddr == "" {
		klog.Fatal("ufm server address is empty")
	}

	if username == "" || password == "" {
		klog.Fatal("username or password is empty")
	}

	data, err := ufmData(ufmAddr, username, password)
	if err != nil {
		klog.ErrorS(err, "Failed to get ufm data, use mock data")
	}
	leafSwitches := LeafSwitchesGroups(data)
	buildAndCreateHyperNode(vcClient, leafSwitches)
}

func buildAndCreateHyperNode(client vcclientset.Interface, leafSwitches map[string]LeafSwitchesGroup) {
	leafHyperNodeNames := make([]string, 0, len(leafSwitches))
	for groupID, leafs := range leafSwitches {
		klog.InfoS("LeafSwitches groups", "groupID", groupID)
		for leafName, leaf := range leafs.Leafs {
			klog.InfoS("leafSwitches groups", "leafName", leafName, "nodes", leaf.NodeNames.UnsortedList())
		}

		hnName := fmt.Sprintf("leaf-hn-%s", groupID)
		nodeList := leafs.NodeNames.UnsortedList()
		klog.InfoS("Begin to create leaf leafHyperNode", "name", hnName, "members", nodeList)
		members := util.GetMembers(nodeList, topologyv1alpha1.MemberTypeNode)
		leafHyperNode := util.BuildHyperNode(hnName, 1, members)
		if err := util.CreateHyperNode(client, leafHyperNode); err != nil {
			klog.ErrorS(err, "Failed to create leafHyperNode", "name", hnName)
			continue
		}

		leafHyperNodeNames = append(leafHyperNodeNames, hnName)
	}

	// create spine hyperNode
	hnName := "spine-hn"
	klog.InfoS("Begin to create spine leafHyperNode", "name", hnName, "members", leafHyperNodeNames)
	members := util.GetMembers(leafHyperNodeNames, topologyv1alpha1.MemberTypeHyperNode)
	spineHyperNode := util.BuildHyperNode(hnName, 2, members)
	if err := util.CreateHyperNode(client, spineHyperNode); err != nil {
		klog.ErrorS(err, "Failed to create spine hyperNode")
		return
	}
}

func ufmData(ufmAddr, username, password string) ([]UFMInterface, error) {
	// Build request URL
	u := &url.URL{
		Scheme: "https",
		Host:   strings.TrimRight(ufmAddr, "/"),
		Path:   "/ufmRest/resources/ports",
	}
	url := u.String()

	// Create HTTP request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return mockUfmData(), err
	}

	// Set basic authentication
	req.SetBasicAuth(username, password)

	// Skip HTTPS certificate verification
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	klog.InfoS("WARNING: TLS certificate verification is disabled which is insecure. This should not be used in production environments",
		"url", url)

	client := &http.Client{
		Transport: tr,
	}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		klog.ErrorS(err, "Failed to request ufm server", "url", url)
		return mockUfmData(), err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return mockUfmData(), errors.New(resp.Status)
	}

	resp.Body = http.MaxBytesReader(nil, resp.Body, maxBodySize)
	var interfaces []UFMInterface
	if err = json.NewDecoder(resp.Body).Decode(&interfaces); err != nil {
		return mockUfmData(), err
	}

	klog.InfoS("Successfully retrieved UFM data", "count", len(interfaces))
	return interfaces, nil
}

// mockUfmData returns test data for development or when UFM API fails
func mockUfmData() []UFMInterface {
	data := []UFMInterface{
		// group0
		{SystemName: "node0", PeerNodeName: "leaf0", NodeDescription: "node00-x", Description: "Computer IB Port"},
		{SystemName: "node0", PeerNodeName: "leaf1", NodeDescription: "node00-x", Description: "Computer IB Port"},
		{SystemName: "node0", PeerNodeName: "leaf2", NodeDescription: "node00-x", Description: "Computer IB Port"},
		{SystemName: "node0", PeerNodeName: "leaf3", NodeDescription: "node00-x", Description: "Computer IB Port"},

		{SystemName: "node1", PeerNodeName: "leaf0", NodeDescription: "node01-x", Description: "Computer IB Port"},
		{SystemName: "node1", PeerNodeName: "leaf1", NodeDescription: "node01-x", Description: "Computer IB Port"},
		{SystemName: "node1", PeerNodeName: "leaf2", NodeDescription: "node01-x", Description: "Computer IB Port"},
		{SystemName: "node1", PeerNodeName: "leaf3", NodeDescription: "node01-x", Description: "Computer IB Port"},

		{SystemName: "node2", PeerNodeName: "leaf0", NodeDescription: "node02-x", Description: "Computer IB Port"},
		{SystemName: "node2", PeerNodeName: "leaf1", NodeDescription: "node02-x", Description: "Computer IB Port"},
		{SystemName: "node2", PeerNodeName: "leaf2", NodeDescription: "node02-x", Description: "Computer IB Port"},
		{SystemName: "node2", PeerNodeName: "leaf3", NodeDescription: "node02-x", Description: "Computer IB Port"},

		{SystemName: "node3", PeerNodeName: "leaf0", NodeDescription: "node03-x", Description: "Computer IB Port"},
		{SystemName: "node3", PeerNodeName: "leaf1", NodeDescription: "node03-x", Description: "Computer IB Port"},
		{SystemName: "node3", PeerNodeName: "leaf2", NodeDescription: "node03-x", Description: "Computer IB Port"},
		{SystemName: "node3", PeerNodeName: "leaf3", NodeDescription: "node03-x", Description: "Computer IB Port"},

		// group1
		{SystemName: "node4", PeerNodeName: "leaf4", NodeDescription: "node14-x", Description: "Computer IB Port"},
		{SystemName: "node4", PeerNodeName: "leaf5", NodeDescription: "node14-x", Description: "Computer IB Port"},
		{SystemName: "node4", PeerNodeName: "leaf6", NodeDescription: "node14-x", Description: "Computer IB Port"},

		{SystemName: "node5", PeerNodeName: "leaf4", NodeDescription: "node15-x", Description: "Computer IB Port"},
		{SystemName: "node5", PeerNodeName: "leaf5", NodeDescription: "node15-x", Description: "Computer IB Port"},
		{SystemName: "node5", PeerNodeName: "leaf6", NodeDescription: "node15-x", Description: "Computer IB Port"},
		{SystemName: "node5", PeerNodeName: "leaf7", NodeDescription: "node15-x", Description: "Computer IB Port"},

		{SystemName: "node6", PeerNodeName: "leaf4", NodeDescription: "node16-x", Description: "Computer IB Port"},
		{SystemName: "node6", PeerNodeName: "leaf5", NodeDescription: "node16-x", Description: "Computer IB Port"},
		{SystemName: "node6", PeerNodeName: "leaf6", NodeDescription: "node16-x", Description: "Computer IB Port"},
		{SystemName: "node6", PeerNodeName: "leaf7", NodeDescription: "node16-x", Description: "Computer IB Port"},

		{SystemName: "node7", PeerNodeName: "leaf4", NodeDescription: "node17-x", Description: "Computer IB Port"},
		{SystemName: "node7", PeerNodeName: "leaf5", NodeDescription: "node17-x", Description: "Computer IB Port"},
		{SystemName: "node7", PeerNodeName: "leaf6", NodeDescription: "node17-x", Description: "Computer IB Port"},
	}
	return data
}

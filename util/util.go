package util

import (
	"context"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"
)

func CommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	prefix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for ; i < len(prefix) && i < len(s) && prefix[i] == s[i]; i++ {
		}
		prefix = prefix[:i]
		if prefix == "" {
			return ""
		}
	}
	return prefix
}

func InitClient(kubeConfig string) (*vcclientset.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		return nil, err
	}
	vcClient := vcclientset.NewForConfigOrDie(cfg)
	return vcClient, nil
}

func GetMembers(list []string, memberType topologyv1alpha1.MemberType) []topologyv1alpha1.MemberSpec {
	members := make([]topologyv1alpha1.MemberSpec, len(list))
	for i, name := range list {
		members[i] = topologyv1alpha1.MemberSpec{
			Type:     memberType,
			Selector: topologyv1alpha1.MemberSelector{ExactMatch: &topologyv1alpha1.ExactMatch{Name: name}},
		}
	}
	return members
}

func BuildHyperNode(name string, tier int, Members []topologyv1alpha1.MemberSpec) *topologyv1alpha1.HyperNode {
	return &topologyv1alpha1.HyperNode{
		TypeMeta: v1.TypeMeta{
			APIVersion: "topology.volcano.sh/v1alpha1",
			Kind:       "HyperNode",
		},
		ObjectMeta: v1.ObjectMeta{
			Name: name,
		},
		Spec: topologyv1alpha1.HyperNodeSpec{
			Tier:    tier,
			Members: Members,
		},
	}
}

func CreateHyperNode(client vcclientset.Interface, hyperNode *topologyv1alpha1.HyperNode) error {
	err := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return !k8serrors.IsAlreadyExists(err)
	}, func() error {
		_, err := client.TopologyV1alpha1().HyperNodes().Create(context.TODO(), hyperNode, v1.CreateOptions{})
		return err
	})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

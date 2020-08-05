/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package benchmark

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/integration/framework"
	testutils "k8s.io/kubernetes/test/utils"
	"sigs.k8s.io/yaml"
)

const (
	configFile = "config/performance-config.yaml"
)

var (
	defaultMetricsCollectorConfig = metricsCollectorConfig{
		Metrics: []string{
			"scheduler_scheduling_algorithm_predicate_evaluation_seconds",
			"scheduler_scheduling_algorithm_priority_evaluation_seconds",
			"scheduler_binding_duration_seconds",
			"scheduler_e2e_scheduling_duration_seconds",
			"scheduler_scheduling_algorithm_preemption_evaluation_seconds",
			"scheduler_pod_scheduling_duration_seconds",
		},
	}
)

// testCase defines a set of test cases that intend to test the performance of
// similar workloads of varying sizes with shared overall settings such as
// feature gates and metrics collected.
type testCase struct {
	// Name of the testCase.
	Name string
	// Feature gates to set before running the test. Optional.
	FeatureGates map[featuregate.Feature]bool
	// List of metrics to collect. Optional, defaults to
	// defaultMetricsCollectorConfig if unspecified.
	MetricsCollectorConfig *metricsCollectorConfig
	// List of workloads to run under this testCase.
	Workloads []*workload
	// TODO(adtac): reduce config toil by having a default pod and node spec per
	// testCase? CreatePods and CreateNodes ops will inherit these unless
	// manually overridden.
}

// workload is a subtest under a testCase that tests the scheduler performance
// for a certain ordering of ops. The set of nodes created and pods scheduled
// in a workload may be heterogenous.
type workload struct {
	// Name of the workload.
	Name string
	// A list of ops to be executed serially. Each element of the list must be
	// createNodesOp, createPodsOp, or barrierOp.
	Ops []op
}

func (w *workload) collectsMetrics() bool {
	for _, op := range w.Ops {
		if op.realOp.collectsMetrics() {
			return true
		}
	}
	return false
}

// op is a dummy struct which stores the real op in itself.
type op struct {
	realOp realOp
}

// UnmarshalJSON is a custom unmarshaler for the op struct since we don't know
// which op we're decoding at runtime.
func (op *op) UnmarshalJSON(b []byte) error {
	possibleOps := []realOp{
		&createNodesOp{},
		&createPodsOp{},
		&barrierOp{},
		// TODO(adtac): add a sleep timer op to simulate user action?
	}
	var firstError error
	for _, possibleOp := range possibleOps {
		if err := json.Unmarshal(b, possibleOp); err == nil {
			if err2 := possibleOp.isValid(); err2 == nil {
				op.realOp = possibleOp
				return nil
			} else if firstError == nil {
				// Don't return an error yet. Even though this op is invalid, it may
				// still match other possible ops.
				firstError = err2
			}
		}
	}
	return fmt.Errorf("cannot unmarshal %s into any known op type: %v", string(b), firstError)
}

// realOp is an interface that is implemented by different structs. To evaluate
// the validity of ops at parse-time, a isValid function must be implemented.
type realOp interface {
	// isValid verifies the validity of the op args such as node/pod count.
	isValid() error
	// collectsMetrics checks if the op collects metrics.
	collectsMetrics() bool
}

// createNodesOp defines an op where nodes are created as a part of a workload.
type createNodesOp struct {
	// Number of nodes to create.
	CreateNodes int
	// Path to spec file describing the nodes to create. Optional.
	NodeTemplatePath *string
	// At most one of the following strategies can be defined. Optional, defaults
	// to TrivialNodePrepareStrategy if unspecified.
	NodeAllocatableStrategy  *testutils.NodeAllocatableStrategy
	LabelNodePrepareStrategy *testutils.LabelNodePrepareStrategy
	UniqueNodeLabelStrategy  *testutils.UniqueNodeLabelStrategy
}

func (cno *createNodesOp) isValid() error {
	if cno.CreateNodes <= 0 {
		return fmt.Errorf("number of nodes cannot be non-positive")
	}
	return nil
}

func (*createNodesOp) collectsMetrics() bool {
	return false
}

// createPodsOp defines an op where pods are scheduled as a part of a workload.
// The test can block on the completion of this op before moving forward or
// continue asynchronously.
type createPodsOp struct {
	// Number of pods to schedule.
	CreatePods int
	// Whether or not to enable metrics collection for this createPodsOp.
	// Optional. Both CollectMetrics and SkipWaitToCompletion cannot be true at
	// the same time for a particular createPodsOp.
	CollectMetrics bool
	// Namespace the pods should be created in. Optional, defaults to a unique
	// namespace of the format "namespace-<number>".
	Namespace *string
	// Path to spec file describing the pods to schedule. Optional.
	PodTemplatePath *string
	// Whether or not to wait for all pods in this op to get scheduled. Optional,
	// defaults to false.
	SkipWaitToCompletion bool
	// Persistent volume settings for the pods to be scheduled. Optional.
	PersistentVolumeTemplatePath      *string
	PersistentVolumeClaimTemplatePath *string
}

func (cpo *createPodsOp) isValid() error {
	if cpo.CreatePods <= 0 {
		return fmt.Errorf("number of pods cannot be non-positive")
	}
	return nil
}

func (cpo *createPodsOp) collectsMetrics() bool {
	return cpo.CollectMetrics
}

// barrierOp defines an op that can be used to wait until all scheduled pods of
// one or many namespaces have been bound to nodes. This is useful when pods
// were scheduled with SkipWaitToCompletion set to true. A barrierOp is added
// at the end of each each workload automatically.
type barrierOp struct {
	// Namespaces to block on. Optional, defaults to all namespaces.
	Barrier []string
}

func (bo *barrierOp) isValid() error {
	return nil
}

func (*barrierOp) collectsMetrics() bool {
	return false
}

func BenchmarkPerfScheduling(b *testing.B) {
	testCases, err := getTestCases(configFile)
	if err != nil {
		b.Fatal(err)
	}
	if err = validateTestCases(testCases); err != nil {
		b.Fatal(err)
	}

	dataItems := DataItems{Version: "v1"}
	for _, tc := range testCases {
		b.Run(tc.Name, func(b *testing.B) {
			for _, w := range tc.Workloads {
				b.Run(w.Name, func(b *testing.B) {
					for feature, flag := range tc.FeatureGates {
						resetFunc := featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, feature, flag)
						defer resetFunc()
					}
					dataItems.DataItems = append(dataItems.DataItems, runWorkload(b, tc, w)...)
				})
			}
		})
	}
	if err := dataItems2JSONFile(dataItems, b.Name()); err != nil {
		klog.Fatalf("%v: unable to write measured data: %v", b.Name(), err)
	}
}

func runWorkload(b *testing.B, tc *testCase, w *workload) []DataItem {
	var mu sync.Mutex
	var dataItems []DataItem
	finalFunc, podInformer, clientset := mustSetupScheduler()
	defer finalFunc()

	numPodsScheduledPerNamespace := make(map[string]int)
	numNodes := 0
	allPodsAsync := true

	b.StopTimer()
	b.ResetTimer()
	for opIndex, op := range w.Ops {
		switch realOp := op.realOp.(type) {
		case *createNodesOp:
			nodePreparer, err := getNodePreparer(fmt.Sprintf("node-%d-", opIndex), realOp, clientset)
			if err != nil {
				b.Fatal(err)
			}
			if err := nodePreparer.PrepareNodes(numNodes); err != nil {
				b.Fatal(err)
			}
			if numNodes == 0 {
				// Schedule a cleanup at most once. The CleanupNodes function will list
				// and delete *all* nodes.
				// TODO(adtac): make CleanupNodes only clean up its own nodes to make
				// this more intuitive?
				defer nodePreparer.CleanupNodes()
			}
			numNodes += realOp.CreateNodes

		case *createPodsOp:
			var namespace string
			if realOp.Namespace != nil {
				namespace = *realOp.Namespace
			} else {
				namespace = fmt.Sprintf("namespace-%d", opIndex)
			}
			var stopCh chan struct{}
			var collectors []testDataCollector
			if realOp.CollectMetrics {
				b.StartTimer()
				stopCh = make(chan struct{})
				collectors = getTestDataCollectors(podInformer, fmt.Sprintf("%s/%s", b.Name(), namespace), namespace, tc.MetricsCollectorConfig)
				for _, collector := range collectors {
					go collector.run(stopCh)
				}
			}
			if err := createPods(namespace, realOp, clientset); err != nil {
				b.Fatal(err)
			}
			finish := func() {
				if err := barrierOne(podInformer, b.Name(), namespace, realOp.CreatePods); err != nil {
					b.Fatal(err)
				}
				if realOp.CollectMetrics {
					close(stopCh)
					mu.Lock()
					for _, collector := range collectors {
						dataItems = append(dataItems, collector.collect()...)
					}
					mu.Unlock()
				}
			}
			if realOp.SkipWaitToCompletion {
				// Only record those namespaces that may potentially require barriers
				// in the future.
				if _, ok := numPodsScheduledPerNamespace[namespace]; ok {
					numPodsScheduledPerNamespace[namespace] += realOp.CreatePods
				} else {
					numPodsScheduledPerNamespace[namespace] = realOp.CreatePods
				}
				go finish()
				// We can't stop the timer after the pods actually get scheduled
				// because a different timer may have been started and stopped by then.
				// AFAIK, there is no way to add the time elapsed to the benchmark's
				// timer concurrently without using StartTimer and StopTimer.
				b.StopTimer()
			} else {
				allPodsAsync = false
				finish()
				b.StopTimer()
			}

		case *barrierOp:
			for _, barrier := range realOp.Barrier {
				if _, ok := numPodsScheduledPerNamespace[barrier]; !ok {
					b.Fatalf("unknown namespace %s", barrier)
				}
			}
			if err := barrier(podInformer, b.Name(), realOp.Barrier, numPodsScheduledPerNamespace); err != nil {
				b.Fatal(err)
			}
			// At the end of the barrier, we can be sure that there are no pods
			// pending scheduling in the namespaces that we just blocked on.
			if len(realOp.Barrier) == 0 {
				numPodsScheduledPerNamespace = make(map[string]int)
			} else {
				for _, namespace := range realOp.Barrier {
					delete(numPodsScheduledPerNamespace, namespace)
				}
			}
		default:
			b.Fatalf("invalid op %v", realOp)
		}
	}

	if allPodsAsync {
		// The go-test timer cannot measure pods being scheduled in the background
		// (specified through the skipWaitToCompletion option). As a result, if all
		// pods were scheduled in the background, go-test will think the test
		// completed nearly instantaneously; to work around this, in such
		// scenarios, we measure the actual time to completion using the final
		// barrier. Note that there will always be at least one CreatePods op that
		// will start and stop the timer, so workloads with at least synchronous
		// pod creation will not need this workaround.
		b.StartTimer()
	}
	if err := barrier(podInformer, b.Name(), nil, numPodsScheduledPerNamespace); err != nil {
		// Any pending pods must be scheduled before this test can be considered to
		// be complete.
		b.Fatal(err)
	}
	if allPodsAsync {
		b.StopTimer()
	}

	return dataItems
}

type testDataCollector interface {
	run(stopCh chan struct{})
	collect() []DataItem
}

func getTestDataCollectors(podInformer coreinformers.PodInformer, name, namespace string, mcc *metricsCollectorConfig) []testDataCollector {
	if mcc == nil {
		mcc = &defaultMetricsCollectorConfig
	}
	return []testDataCollector{
		newThroughputCollector(podInformer, map[string]string{"Name": name}, []string{namespace}),
		newMetricsCollector(mcc, map[string]string{"Name": name}),
	}
}

func getNodePreparer(prefix string, cno *createNodesOp, clientset clientset.Interface) (testutils.TestNodePreparer, error) {
	var nodeStrategy testutils.PrepareNodeStrategy = &testutils.TrivialNodePrepareStrategy{}
	if cno.NodeAllocatableStrategy != nil {
		nodeStrategy = cno.NodeAllocatableStrategy
	} else if cno.LabelNodePrepareStrategy != nil {
		nodeStrategy = cno.LabelNodePrepareStrategy
	} else if cno.UniqueNodeLabelStrategy != nil {
		nodeStrategy = cno.UniqueNodeLabelStrategy
	}

	if cno.NodeTemplatePath != nil {
		node, err := getNodeSpecFromFile(cno.NodeTemplatePath)
		if err != nil {
			return nil, err
		}
		return framework.NewIntegrationTestNodePreparerWithNodeSpec(
			clientset,
			[]testutils.CountToStrategy{{Count: cno.CreateNodes, Strategy: nodeStrategy}},
			node,
		), nil
	}
	return framework.NewIntegrationTestNodePreparer(
		clientset,
		[]testutils.CountToStrategy{{Count: cno.CreateNodes, Strategy: nodeStrategy}},
		prefix,
	), nil
}

func createPods(namespace string, cpo *createPodsOp, clientset clientset.Interface) error {
	strategy, err := getPodStrategy(cpo)
	if err != nil {
		return err
	}
	config := testutils.NewTestPodCreatorConfig()
	config.AddStrategy(namespace, cpo.CreatePods, strategy)
	podCreator := testutils.NewTestPodCreator(clientset, config)
	return podCreator.CreatePods()
}

// barrierOne blocks until all pods in the given namespace are scheduled.
func barrierOne(podInformer coreinformers.PodInformer, name string, namespace string, wantCount int) error {
	for {
		scheduled, err := getScheduledPods(podInformer, namespace)
		if err != nil {
			return err
		}
		if len(scheduled) >= wantCount {
			break
		}
		klog.Infof("%s: namespace %s: got %d existing pods, want %d", name, namespace, len(scheduled), wantCount)
		time.Sleep(1 * time.Second)
	}
	return nil
}

// barrier blocks until the all pods in the given namespaces are scheduled.
func barrier(podInformer coreinformers.PodInformer, name string, namespaces []string, numPodsScheduledPerNamespace map[string]int) error {
	// If unspecified, default to all known namespaces.
	if namespaces == nil {
		for namespace := range numPodsScheduledPerNamespace {
			namespaces = append(namespaces, namespace)
		}
	}

	for _, namespace := range namespaces {
		wantCount, ok := numPodsScheduledPerNamespace[namespace]
		if !ok {
			return fmt.Errorf("unknown namespace %s", namespace)
		}
		if err := barrierOne(podInformer, name, namespace, wantCount); err != nil {
			return err
		}
	}
	return nil
}

func getSpecFromFile(path *string, spec interface{}) error {
	bytes, err := ioutil.ReadFile(*path)
	if err != nil {
		return err
	}
	return yaml.UnmarshalStrict(bytes, spec)
}

func getTestCases(path string) ([]*testCase, error) {
	testCases := make([]*testCase, 0)
	if err := getSpecFromFile(&path, &testCases); err != nil {
		return nil, fmt.Errorf("parsing test cases: %v", err)
	}
	return testCases, nil
}

func validateTestCases(testCases []*testCase) error {
	if len(testCases) == 0 {
		return fmt.Errorf("no test cases defined")
	}
	for _, tc := range testCases {
		if len(tc.Workloads) == 0 {
			return fmt.Errorf("%s: no workloads defined", tc.Name)
		}
		for _, w := range tc.Workloads {
			if len(w.Ops) == 0 {
				return fmt.Errorf("%s/%s: no ops defined", tc.Name, w.Name)
			}
			// Make sure there's at least one CreatePods op with collectMetrics set
			// to true in each workload. What's the point of running a performance
			// benchmark if no statistics are collected for reporting?
			if !w.collectsMetrics() {
				return fmt.Errorf("%s/%s: none of the ops collect metrics", tc.Name, w.Name)
			}
			// TODO(adtac): make sure each workload within a test case has a unique
			// name? The name is used to identify the stats in benchmark reports.
		}
	}
	return nil
}

func getPodStrategy(cpo *createPodsOp) (testutils.TestPodCreateStrategy, error) {
	basePod := makeBasePod()
	if cpo.PodTemplatePath != nil {
		var err error
		basePod, err = getPodSpecFromFile(cpo.PodTemplatePath)
		if err != nil {
			return nil, err
		}
	}
	if cpo.PersistentVolumeClaimTemplatePath == nil {
		return testutils.NewCustomCreatePodStrategy(basePod), nil
	}

	pvTemplate, err := getPersistentVolumeSpecFromFile(cpo.PersistentVolumeTemplatePath)
	if err != nil {
		return nil, err
	}
	pvcTemplate, err := getPersistentVolumeClaimSpecFromFile(cpo.PersistentVolumeClaimTemplatePath)
	if err != nil {
		return nil, err
	}
	return testutils.NewCreatePodWithPersistentVolumeStrategy(pvcTemplate, getCustomVolumeFactory(pvTemplate), basePod), nil
}

func getNodeSpecFromFile(path *string) (*v1.Node, error) {
	nodeSpec := &v1.Node{}
	if err := getSpecFromFile(path, nodeSpec); err != nil {
		return nil, fmt.Errorf("parsing Node: %v", err)
	}
	return nodeSpec, nil
}

func getPodSpecFromFile(path *string) (*v1.Pod, error) {
	podSpec := &v1.Pod{}
	if err := getSpecFromFile(path, podSpec); err != nil {
		return nil, fmt.Errorf("parsing Pod: %v", err)
	}
	return podSpec, nil
}

func getPersistentVolumeSpecFromFile(path *string) (*v1.PersistentVolume, error) {
	persistentVolumeSpec := &v1.PersistentVolume{}
	if err := getSpecFromFile(path, persistentVolumeSpec); err != nil {
		return nil, fmt.Errorf("parsing PersistentVolume: %v", err)
	}
	return persistentVolumeSpec, nil
}

func getPersistentVolumeClaimSpecFromFile(path *string) (*v1.PersistentVolumeClaim, error) {
	persistentVolumeClaimSpec := &v1.PersistentVolumeClaim{}
	if err := getSpecFromFile(path, persistentVolumeClaimSpec); err != nil {
		return nil, fmt.Errorf("parsing PersistentVolumeClaim: %v", err)
	}
	return persistentVolumeClaimSpec, nil
}

func getCustomVolumeFactory(pvTemplate *v1.PersistentVolume) func(id int) *v1.PersistentVolume {
	return func(id int) *v1.PersistentVolume {
		pv := pvTemplate.DeepCopy()
		volumeID := fmt.Sprintf("vol-%d", id)
		pv.ObjectMeta.Name = volumeID
		pvs := pv.Spec.PersistentVolumeSource
		if pvs.CSI != nil {
			pvs.CSI.VolumeHandle = volumeID
		} else if pvs.AWSElasticBlockStore != nil {
			pvs.AWSElasticBlockStore.VolumeID = volumeID
		}
		return pv
	}
}

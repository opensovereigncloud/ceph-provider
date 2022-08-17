package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/onmetal/controller-utils/buildutils"
	"github.com/onmetal/controller-utils/modutils"
	storagev1alpha1 "github.com/onmetal/onmetal-api/apis/storage/v1alpha1"
	"github.com/onmetal/onmetal-api/envtestutils"
	"github.com/onmetal/onmetal-api/envtestutils/apiserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rookv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	//+kubebuilder:scaffold:imports
)

const (
	slowSpecThreshold    = 10 * time.Second
	eventuallyTimeout    = 3 * time.Second
	pollingInterval      = 50 * time.Millisecond
	consistentlyDuration = 1 * time.Second
	apiServiceTimeout    = 5 * time.Minute

	volumePoolName        = "my-pool"
	volumePoolProviderID  = "custom://pool"
	volumePoolReplication = 3
)

var (
	ctx        = context.Background()
	testEnv    *envtest.Environment
	testEnvExt *envtestutils.EnvironmentExtensions
	cfg        *rest.Config
	k8sClient  client.Client

	volumeClassSelector = map[string]string{
		"suitable-for": "testing",
	}
	volumePoolLabels = map[string]string{
		"some": "label",
	}
	volumePoolAnnotations = map[string]string{
		"some": "annotation",
	}
)

func TestAPIs(t *testing.T) {
	_, reporterConfig := GinkgoConfiguration()
	reporterConfig.SlowSpecThreshold = slowSpecThreshold
	SetDefaultConsistentlyPollingInterval(pollingInterval)
	SetDefaultEventuallyPollingInterval(pollingInterval)
	SetDefaultEventuallyTimeout(eventuallyTimeout)
	SetDefaultConsistentlyDuration(consistentlyDuration)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Cephlet Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true), zap.Level(zapcore.InfoLevel)))

	var err error

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			modutils.Dir("github.com/rook/rook", "deploy", "examples", "crds.yaml"),
		},
		ErrorIfCRDPathMissing: true,
	}

	testEnvExt = &envtestutils.EnvironmentExtensions{
		APIServiceDirectoryPaths: []string{
			modutils.Dir("github.com/onmetal/onmetal-api", "config", "apiserver", "apiservice", "bases"),
		},
		ErrorIfAPIServicePathIsMissing: true,
	}

	cfg, err = envtestutils.StartWithExtensions(testEnv, testEnvExt)
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	DeferCleanup(envtestutils.StopWithExtensions, testEnv, testEnvExt)

	Expect(rookv1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(storagev1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	// Init package-level k8sClient
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	apiSrv, err := apiserver.New(cfg, apiserver.Options{
		MainPath:     "github.com/onmetal/onmetal-api/cmd/apiserver",
		BuildOptions: []buildutils.BuildOption{buildutils.ModModeMod},
		ETCDServers:  []string{testEnv.ControlPlane.Etcd.URL.String()},
		Host:         testEnvExt.APIServiceInstallOptions.LocalServingHost,
		Port:         testEnvExt.APIServiceInstallOptions.LocalServingPort,
		CertDir:      testEnvExt.APIServiceInstallOptions.LocalServingCertDir,
	})
	Expect(err).NotTo(HaveOccurred())

	By("starting the onmetal-api aggregated api server")
	Expect(apiSrv.Start()).To(Succeed())
	DeferCleanup(apiSrv.Stop)

	Expect(envtestutils.WaitUntilAPIServicesReadyWithTimeout(apiServiceTimeout, testEnvExt, k8sClient, scheme.Scheme)).To(Succeed())
})

func SetupTest(ctx context.Context) *corev1.Namespace {
	var (
		cancel context.CancelFunc
	)
	testNamespace := &corev1.Namespace{}
	rookNamespace := &corev1.Namespace{}
	BeforeEach(func() {
		var mgrCtx context.Context
		mgrCtx, cancel = context.WithCancel(ctx)
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "testns-",
			},
		}
		Expect(k8sClient.Create(ctx, testNamespace)).To(Succeed(), "failed to create test namespace")

		rookNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rookns-",
			},
		}
		Expect(k8sClient.Create(ctx, rookNamespace)).To(Succeed(), "failed to create test namespace")

		k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:             scheme.Scheme,
			Host:               "127.0.0.1",
			MetricsBindAddress: "0",
		})
		Expect(err).ToNot(HaveOccurred())

		// register reconciler here
		Expect((&VolumeReconciler{
			Client:                               k8sManager.GetClient(),
			Scheme:                               k8sManager.GetScheme(),
			VolumePoolReplication:                volumePoolReplication,
			VolumePoolName:                       volumePoolName,
			VolumePoolLabels:                     volumePoolLabels,
			VolumePoolAnnotations:                volumePoolAnnotations,
			RookNamespace:                        rookNamespace.Name,
			RookMonitorEndpointConfigMapDataKey:  RookMonitorConfigMapDataKeyDefaultValue,
			RookMonitorEndpointConfigMapName:     RookMonitorConfigMapNameDefaultValue,
			RookCSIRBDProvisionerSecretName:      RookCSIRBDProvisionerSecretNameDefaultValue,
			RookCSIRBDNodeSecretName:             RookCSIRBDNodeSecretNameDefaultValue,
			RookStorageClassAllowVolumeExpansion: RookStorageClassAllowVolumeExpansionDefaultValue,
			RookStorageClassFSType:               RookStorageClassFSTypeDefaultValue,
			RookStoragClassImageFeatures:         RookStorageClassImageFeaturesDefaultValue,
			RookStorageClassMountOptions:         RookStorageClassMountOptionsDefaultValue,
			RookStorageClassReclaimPolicy:        RookStorageClassReclaimPolicyDefaultValue,
			RookStorageClassVolumeBindingMode:    RookStorageClassVolumeBindingModeDefaultValue,
		}).SetupWithManager(k8sManager)).To(Succeed())
		Expect((&VolumePoolReconciler{
			Client:                k8sManager.GetClient(),
			Scheme:                k8sManager.GetScheme(),
			VolumePoolReplication: volumePoolReplication,
			VolumePoolName:        volumePoolName,
			VolumePoolLabels:      volumePoolLabels,
			VolumePoolProviderID:  volumePoolProviderID,
			VolumePoolAnnotations: volumePoolAnnotations,
			VolumeClassSelector:   volumeClassSelector,
			RookNamespace:         rookNamespace.Name,
		}).SetupWithManager(k8sManager)).To(Succeed())

		go func() {
			Expect(k8sManager.Start(mgrCtx)).To(Succeed(), "failed to start manager")
		}()
	})

	AfterEach(func() {
		cancel()
		Expect(k8sClient.Delete(ctx, testNamespace)).To(Succeed(), "failed to delete test namespace")
		Expect(k8sClient.Delete(ctx, rookNamespace)).To(Succeed(), "failed to delete rook namespace")
	})

	return testNamespace
}

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

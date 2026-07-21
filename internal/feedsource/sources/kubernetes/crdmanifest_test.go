package kubernetes_test

import (
	"os"
	"path/filepath"
	"testing"
)

// crdManifestPath locates the standalone Feed CRD manifest, relative to this
// package directory. The k3s integration test applies it to a live API server;
// this constant is shared so the path is asserted by `task test` — which needs
// no Docker — rather than only surfacing in the integration shard.
// The four parent hops walk internal/feedsource/sources/kubernetes back to the repo root.
var crdManifestPath = filepath.Join("..", "..", "..", "..", "deploy", "crds", "feeds.rss2msg.io.yaml")

// TestCRDManifestPathResolves guards the relative path the k3s integration test
// depends on. That test is behind the `integration` build tag and needs Docker,
// so a wrong path here otherwise stays invisible until CI runs the integration
// shard and burns a k3s container before failing on a missing file.
func TestCRDManifestPathResolves(t *testing.T) {
	info, err := os.Stat(crdManifestPath)
	if err != nil {
		t.Fatalf("CRD manifest not found at %s: %v\n"+
			"This path is relative to this package directory and is also used by "+
			"TestKubernetesSourceK3sRoundTrip; if the package moved, the number of "+
			"parent hops needs updating.", crdManifestPath, err)
	}
	if info.IsDir() {
		t.Fatalf("%s is a directory, want the CRD manifest file", crdManifestPath)
	}
}

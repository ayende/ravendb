import { globalDispatch } from "components/storeCompat";
import { clusterActions } from "components/common/shell/clusterSlice";
import { ClusterStubs } from "test/stubs/ClusterStubs";

export class MockClusterManager {
    with_ClientVersion(version: string = ClusterStubs.clientVersion()) {
        globalDispatch(clusterActions.clientVersionLoaded(version));
    }

    with_ServerVersion() {
        globalDispatch(clusterActions.serverVersionLoaded(ClusterStubs.serverVersion()));
    }

    with_PassiveServer(passive: boolean) {
        globalDispatch(clusterActions.serverStateLoaded({ passive }));
    }

    with_Cluster() {
        globalDispatch(
            clusterActions.nodesLoaded([
                {
                    nodeTag: "A",
                    serverUrl: "https://a.server-url.com",
                    type: "Member",
                },
                {
                    nodeTag: "B",
                    serverUrl: "https://b.server-url.com",
                    type: "Member",
                },
                {
                    nodeTag: "C",
                    serverUrl: "https://c.server-url.com",
                    type: "Member",
                },
            ])
        );
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }

    with_Single() {
        globalDispatch(
            clusterActions.nodesLoaded([
                {
                    nodeTag: "A",
                    serverUrl: "https://a.server-url.com",
                    type: "Member",
                },
            ])
        );
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }
}

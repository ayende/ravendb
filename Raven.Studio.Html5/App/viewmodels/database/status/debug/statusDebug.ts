import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/database/status/status");
import shell = require('viewmodels/shell');

class statusDebug extends viewModelBase {
    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();

	    var db = this.activeDatabase();
	    var durandalConfigurationArray: DurandalRouteConfiguration[] =
	    [
	        { route: 'databases/status/debug', moduleId: 'viewmodels/database/status/debug/statusDebugChanges', title: 'Changes', tooltip: "Shows information about active changes API subscriptions", nav: true, hash: appUrl.forCurrentDatabase().statusDebugChanges },
			{ route: 'databases/status/debug/metrics', moduleId: 'viewmodels/database/status/debug/statusDebugMetrics', title: 'Metrics', tooltip: "Shows database metrics", nav: true, hash: appUrl.forCurrentDatabase().statusDebugMetrics },
			{ route: 'databases/status/debug/config', moduleId: 'viewmodels/database/status/debug/statusDebugConfig', title: 'Config', tooltip: "Displays server configuration", nav: true, hash: appUrl.forCurrentDatabase().statusDebugConfig },
			{ route: 'databases/status/debug/docrefs', moduleId: 'viewmodels/database/status/debug/statusDebugDocrefs', title: 'Doc refs', tooltip: "Allows to find documents referenced by given document id", nav: true, hash: appUrl.forCurrentDatabase().statusDebugDocrefs },
			{ route: 'databases/status/debug/currentlyIndexing', moduleId: 'viewmodels/database/status/debug/statusDebugCurrentlyIndexing', title: 'Currently indexing', tooltip: "Displays currently performed indexing work", nav: true, hash: appUrl.forCurrentDatabase().statusDebugCurrentlyIndexing },
			{ route: 'databases/status/debug/queries', moduleId: 'viewmodels/database/status/debug/statusDebugQueries', title: 'Queries', tooltip: "Displays currently running queries", nav: true, hash: appUrl.forCurrentDatabase().statusDebugQueries },
			{ route: 'databases/status/debug/tasks', moduleId: 'viewmodels/database/status/debug/statusDebugTasks', title: 'Tasks', tooltip: "Displays currently running index tasks", nav: true, hash: appUrl.forCurrentDatabase().statusDebugTasks },
			{ route: 'databases/status/debug/routes', moduleId: 'viewmodels/database/status/debug/statusDebugRoutes', title: 'Routes', tooltip: "Displays all available routes", nav: shell.isGlobalAdmin(), hash: appUrl.forCurrentDatabase().statusDebugRoutes },
			{ route: 'databases/status/debug/sqlReplication', moduleId: 'viewmodels/database/status/debug/statusDebugSqlReplication', title: 'SQL Replication', tooltip: "Shows information about SQL replication", nav: db.isBundleActive("SqlReplication"), hash: appUrl.forCurrentDatabase().statusDebugSqlReplication },
			{ route: 'databases/status/debug/indexFields', moduleId: 'viewmodels/database/status/debug/statusDebugIndexFields', title: 'Index fields', tooltip: "Shows names of indexed fields based on entered index definition", nav: true, hash: appUrl.forCurrentDatabase().statusDebugIndexFields },
			{ route: 'databases/status/debug/identities', moduleId: 'viewmodels/database/status/debug/statusDebugIdentities', title: 'Identities', tooltip: "Shows identities values for collections", nav: true, hash: appUrl.forCurrentDatabase().statusDebugIdentities },
			{ route: 'databases/status/debug/websocket', moduleId: 'viewmodels/database/status/debug/statusDebugWebSocket', title: 'Web Socket', tooltip: "Allows to debug websockets connection", nav: true, hash: appUrl.forCurrentDatabase().statusDebugWebSocket },
			{ route: 'databases/status/debug/persist', moduleId: 'viewmodels/database/status/debug/statusDebugPersistAutoIndex', title: 'Persist auto index', tooltip: "Persists auto index", nav: true, hash: appUrl.forCurrentDatabase().statusDebugPersistAutoIndex },
			{ route: 'databases/status/debug/explainReplication', moduleId: 'viewmodels/database/status/debug/statusDebugExplainReplication', title: 'Explain replication', tooltip: "Shows information about replication of given document to given replication destination", nav: db.isBundleActive("Replication"), hash: appUrl.forCurrentDatabase().statusDebugExplainReplication },
	    ];

        this.router = status.statusRouter.createChildRouter()
            .map(durandalConfigurationArray)
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = statusDebug;    
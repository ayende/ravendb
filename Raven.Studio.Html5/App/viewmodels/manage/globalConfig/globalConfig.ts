import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import adminSettings = require("viewmodels/manage/adminSettings");

class globalConfig extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();

        this.router = adminSettings.adminSettingsRouter.createChildRouter()
            .map([
                { route: "admin/settings/globalConfig", moduleId: "viewmodels/manage/globalConfig/globalConfigPeriodicExport", title: "Periodic export", tooltip: "", nav: true, hash: appUrl.forGlobalConfigPeriodicExport() },
                { route: "admin/settings/globalConfigReplication", moduleId: "viewmodels/manage/globalConfig/globalConfigReplications", title: "Replication", tooltip: "Global replication settings", nav: true, hash: appUrl.forGlobalConfigReplication() },
                { route: "admin/settings/globalConfigSqlReplication", moduleId: "viewmodels/manage/globalConfig/globalConfigSqlReplication", title: "SQL Replication", tooltip: "Global SQL replication settings", nav: true, hash: appUrl.forGlobalConfigSqlReplication()},
                { route: "admin/settings/globalConfigQuotas", moduleId: "viewmodels/manage/globalConfig/globalConfigQuotas", title: "Quotas", tooltip: "Global quotas settings", nav: true, hash: appUrl.forGlobalConfigQuotas() },
                { route: "admin/settings/globalConfigCustomFunctions", moduleId: "viewmodels/manage/globalConfig/globalConfigCustomFunctions", title: "Custom functions", tooltip: "Global custom functions settings", nav: true, hash: appUrl.forGlobalConfigCustomFunctions() },
                { route: "admin/settings/globalConfigVersioning", moduleId: "viewmodels/manage/globalConfig/globalConfigVersioning", title: "Versioning", tooltip: "Global versioning settings", nav: true, hash: appUrl.forGlobalConfigVersioning() }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = globalConfig;    
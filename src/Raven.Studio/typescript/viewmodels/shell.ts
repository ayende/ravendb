/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import viewLocator = require("durandal/viewLocator");

import menu = require("common/shell/menu");
import resourceSwitcher = require("common/shell/resourceSwitcher");
import searchBox = require("common/shell/searchBox");
import resource = require("models/resources/resource");
import database = require("models/resources/database");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import documentClass = require("models/database/documents/document");
import collection = require("models/database/documents/collection");
import uploadItem = require("models/filesystem/uploadItem");
import changeSubscription = require("common/changeSubscription");
import license = require("models/auth/license");
import topology = require("models/database/replication/topology");
import environmentColor = require("models/resources/environmentColor");

import appUrl = require("common/appUrl");
import uploadQueueHelper = require("common/uploadQueueHelper");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import pagedList = require("common/pagedList");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import enableResizeBindingHandler = require("common/bindingHelpers/enableResizeBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");
import apiKeyLocalStorage = require("common/apiKeyLocalStorage");
import extensions = require("common/extensions");
import serverBuildReminder = require("common/serverBuildReminder");
import eventSourceSettingStorage = require("common/eventSourceSettingStorage");

import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import getLatestServerBuildVersionCommand = require("commands/database/studio/getLatestServerBuildVersionCommand");
import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getFileSystemsCommand = require("commands/filesystem/getFileSystemsCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import getCounterStorageStatsCommand = require("commands/counter/getCounterStorageStatsCommand");
import getTimeSeriesCommand = require("commands/timeSeries/getTimeSeriesCommand");
import getTimeSeriesStatsCommand = require("commands/timeSeries/getTimeSeriesStatsCommand");
import getSystemDocumentCommand = require("commands/database/documents/getSystemDocumentCommand");
import getServerConfigsCommand = require("commands/database/studio/getServerConfigsCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import getStudioConfig = require("commands/getStudioConfig");

import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import licensingStatus = require("viewmodels/common/licensingStatus");
import recentErrors = require("viewmodels/common/recentErrors");
import enterApiKey = require("viewmodels/common/enterApiKey");
import latestBuildReminder = require("viewmodels/common/latestBuildReminder");
import recentQueriesStorage = require("common/recentQueriesStorage");
import getHotSpareInformation = require("commands/licensing/GetHotSpareInformation");

class shell extends viewModelBase {
    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";
    static selectedEnvironmentColorStatic = ko.observable<environmentColor>(new environmentColor("Default", "#f8f8f8"));
    static originalEnvironmentColor = ko.observable<environmentColor>(shell.selectedEnvironmentColorStatic());
    selectedColor = shell.selectedEnvironmentColorStatic;
    selectedEnvironmentText = ko.computed(() => this.selectedColor().name + " Environment");
    canShowEnvironmentText = ko.computed(() => this.selectedColor().name !== "Default");

    renewOAuthTokenTimeoutId: number;
    showContinueTestButton = ko.computed(() => viewModelBase.hasContinueTestOption()); //TODO:
    showLogOutButton: KnockoutComputed<boolean>; //TODO:
    isLoadingStatistics = ko.computed(() => !!this.lastActivatedResource() && !this.lastActivatedResource().statistics()).extend({ throttle: 100 });

    static databases = ko.observableArray<database>();
    databasesLoadedTask: JQueryPromise<any>;
    static fileSystems = ko.observableArray<fileSystem>();
    static counterStorages = ko.observableArray<counterStorage>();
    static timeSeries = ko.observableArray<timeSeries>();

    static resources = ko.computed<resource[]>(() => { //TODO: delete me ?
        var databases: resource[] = shell.databases();
        var fileSystems: resource[] = shell.fileSystems();
        var counterStorages: resource[] = shell.counterStorages();
        var timeSeries: resource[] = shell.timeSeries();
        var result = databases.concat(counterStorages, timeSeries, fileSystems);
        return result.sort((a, b) => {
            return a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1;
        });
    });

    currentConnectedResource: resource;
    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    static clusterMode = ko.observable<boolean>(false);
    isInCluster = ko.computed(() => shell.clusterMode());
    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    static serverMainVersion = ko.observable<number>(4);
    clientBuildVersion = ko.observable<clientBuildVersionDto>();

    windowHeightObservable: KnockoutObservable<number>;
    recordedErrors = ko.observableArray<alertArgs>();
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    showSplash = viewModelBase.showSplash;

    licenseStatus = license.licenseCssClass;
    supportStatus = license.supportCssClass;

    mainMenu = new menu({
        canExposeConfigOverTheWire: accessHelper.canExposeConfigOverTheWire,
        isGlobalAdmin: accessHelper.isGlobalAdmin,
        activeDatabase: this.activeDatabase
    });
    searchBox = new searchBox();
    resourceSwitcher = new resourceSwitcher();

    private globalChangesApi: changesApi;
    private static changeSubscriptionArray: changeSubscription[];

    constructor() {
        super();

        this.preLoadRecentErrorsView();
        extensions.install();

        this.showLogOutButton = ko.computed(() => {
            var lsApiKey = apiKeyLocalStorage.get();
            var contextApiKey = oauthContext.apiKey();
            return lsApiKey || contextApiKey;
        });
        oauthContext.enterApiKeyTask = this.setupApiKey();
        oauthContext.enterApiKeyTask.done(() => {
            /* TODO  this.globalChangesApi = new changesApi(appUrl.getSystemDatabase());
             this.notifications = this.createNotifications();*/
        });

        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("LoadProgress", (alertType?: alertType) => this.dataLoadProgress(alertType));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("SelectNone", () => this.selectNone());
        ko.postbox.subscribe("ActivateDatabase", (db: database) => this.activateDatabase(db));
        ko.postbox.subscribe("ActivateFilesystem", (fs: fileSystem) => this.activateFileSystem(fs));
        ko.postbox.subscribe("ActivateCounterStorage", (cs: counterStorage) => this.activateCounterStorage(cs));
        ko.postbox.subscribe("ActivateTimeSeries", (ts: timeSeries) => this.activateTimeSeries(ts));
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));
        ko.postbox.subscribe("ChangesApiReconnected", (rs: resource) => this.reloadDataAfterReconnection(rs));

        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        enableResizeBindingHandler.install();
        helpBindingHandler.install();

        this.clientBuildVersion.subscribe(v => viewModelBase.clientVersion("4.0." + v.BuildVersion));
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, true);

        oauthContext.enterApiKeyTask.done(() => this.connectToRavenServer());

        NProgress.set(.7);

        this.setupRouting();

        var self = this;

        window.addEventListener("beforeunload", self.destroyChangesApi.bind(self));

        $(window).bind("storage", (e: any) => {
            if (e.originalEvent.key === eventSourceSettingStorage.localStorageName) {
                if (!JSON.parse(e.originalEvent.newValue)) {
                    self.destroyChangesApi();
                } else {
                    // enable changes api
                    this.globalChangesApi = new changesApi(null);
                    this.notifications = this.createNotifications();
                }
            } else if (e.originalEvent.key === apiKeyLocalStorage.localStorageName) {
                this.onLogOut();
            }
        });

        $(window).resize(() => self.lastActivatedResource.valueHasMutated());
        //TODO: return shell.fetchLicenseStatus();

    }

    private setupRouting() {
        let routes = this.getRoutesForNewLayout();
        routes.pushAll(routes);
        router.map(routes).buildNavigationModel();
        router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));
        router.on('router:navigation:cancelled', () => this.showNavigationProgress(false));

        appUrl.mapUnknownRoutes(router);
    }

    private destroyChangesApi() {
        /*TODO
        this.cleanupNotifications();
        shell.disconnectFromResourceChangesApi();*/
    }

    // Called by Durandal when shell.html has been put into the DOM.
    // The view must be attached to the DOM before we can hook up keyboard shortcuts.
    attached() {
        super.attached();

        var target = document.getElementById("splash-spinner");
        this.showSplash.subscribe((show: boolean) => {
            if (show) {
                this.spinner.spin(target);
            } else {
                this.spinner.stop();
            }
        });

        sys.error = (e: any) => {
            console.error(e);
            messagePublisher.reportError("Failed to load routed module!", e);
        };

    }

    private initializeShellComponents() {
        this.mainMenu.initialize();
        this.resourceSwitcher.initialize();
        this.searchBox.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        this.initializeShellComponents();
    }

    private preLoadRecentErrorsView() {
        // preload this view as in case of failure server can't serve it.
        viewLocator.locateView("views/common/recentErrors");
    }

    /*
    static fetchStudioConfig() {
        var hotSpareTask = new getHotSpareInformation().execute();
        var configTask = new getDocumentWithMetadataCommand(shell.studioConfigDocumentId, appUrl.getSystemDatabase(), true).execute();

        $.when(hotSpareTask, configTask).done((hotSpareResult, doc: documentClass) => {
            var hotSpare = <HotSpareDto>hotSpareResult[0];

            appUrl.warnWhenUsingSystemDatabase = doc && doc["WarnWhenUsingSystemDatabase"];

            if (license.licenseStatus().Attributes.hotSpare === "true") {
                // override environment colors with hot spare
                this.activateHotSpareEnvironment(hotSpare);
            } else {
                var envColor = doc && doc["EnvironmentColor"];
                if (envColor != null) {
                    var color = new environmentColor(envColor.Name, envColor.BackgroundColor);
                    shell.selectedEnvironmentColorStatic(color);
                    shell.originalEnvironmentColor(color);
                }
            }
        });
    }*/

    private getRoutesForNewLayout() {
        let routes = [
            {
                route: "databases/upgrade",
                title: "Upgrade in progress",
                moduleId: "viewmodels/common/upgrade",
                nav: false,
                dynamicHash: this.appUrls.upgrade
            },
            {
                route: "databases/edit",
                title: "Edit Document",
                moduleId: "viewmodels/database/documents/editDocument",
                nav: false
            },
            {
                route: "filesystems/files",
                title: "Files",
                moduleId: "viewmodels/filesystem/files/filesystemFiles",
                nav: true,
                dynamicHash: this.appUrls.filesystemFiles
            },
            {
                route: "filesystems/search",
                title: "Search",
                moduleId: "viewmodels/filesystem/search/search",
                nav: true,
                dynamicHash: this.appUrls.filesystemSearch
            },
            {
                route: "filesystems/synchronization*details",
                title: "Synchronization",
                moduleId: "viewmodels/filesystem/synchronization/synchronization",
                nav: true,
                dynamicHash: this.appUrls.filesystemSynchronization
            },
            {
                route: "filesystems/status*details",
                title: "Status",
                moduleId: "viewmodels/filesystem/status/status",
                nav: true,
                dynamicHash: this.appUrls.filesystemStatus
            },
            {
                route: "filesystems/tasks*details",
                title: "Tasks",
                moduleId: "viewmodels/filesystem/tasks/tasks",
                nav: true,
                dynamicHash: this.appUrls.filesystemTasks
            },
            {
                route: "filesystems/settings*details",
                title: "Settings",
                moduleId: "viewmodels/filesystem/settings/settings",
                nav: true,
                dynamicHash: this.appUrls.filesystemSettings
            },
            {
                route: "filesystems/configuration",
                title: "Configuration",
                moduleId: "viewmodels/filesystem/configurations/configuration",
                nav: true,
                dynamicHash: this.appUrls.filesystemConfiguration
            },
            {
                route: "filesystems/edit",
                title: "Edit File",
                moduleId: "viewmodels/filesystem/files/filesystemEditFile",
                nav: false
            },
            {
                route: "counterstorages/counters",
                title: "Counters",
                moduleId: "viewmodels/counter/counters",
                nav: true,
                dynamicHash: this.appUrls.counterStorageCounters
            },
            {
                route: "counterstorages/replication",
                title: "Replication",
                moduleId: "viewmodels/counter/counterStorageReplication",
                nav: true,
                dynamicHash: this.appUrls.counterStorageReplication
            },
            {
                route: "counterstorages/tasks*details",
                title: "Stats",
                moduleId: "viewmodels/counter/tasks/tasks",
                nav: true,
                dynamicHash: this.appUrls.counterStorageStats
            },
            {
                route: "counterstorages/stats",
                title: "Stats",
                moduleId: "viewmodels/counter/counterStorageStats",
                nav: true,
                dynamicHash: this.appUrls.counterStorageStats
            },
            {
                route: "counterstorages/configuration",
                title: "Configuration",
                moduleId: "viewmodels/counter/counterStorageConfiguration",
                nav: true,
                dynamicHash: this.appUrls.counterStorageConfiguration
            },
            {
                route: "counterstorages/edit",
                title: "Edit Counter",
                moduleId: "viewmodels/counter/editCounter",
                nav: false
            },
            {
                route: "timeseries/types",
                title: "Types",
                moduleId: "viewmodels/timeSeries/timeSeriesTypes",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesType
            },
            {
                route: "timeseries/points",
                title: "Points",
                moduleId: "viewmodels/timeSeries/timeSeriesPoints",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesPoints
            },
            {
                route: "timeseries/stats",
                title: "Stats",
                moduleId: "viewmodels/timeSeries/timeSeriesStats",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesStats
            },
            {
                route: "timeseries/configuration*details",
                title: "Configuration",
                moduleId: "viewmodels/timeSeries/configuration/configuration",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesConfiguration
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes.concat(this.mainMenu.routerConfiguration());
    }

    private fecthStudioConfigForDatabase(db: database) {
        var hotSpareTask = new getHotSpareInformation().execute();
        var configTask = new getStudioConfig(db).execute();

        $.when<any>(hotSpareTask, configTask).done((hotSpareResult, docResult) => {
            var hotSpare = hotSpareResult[0];
            var doc = <documentClass>docResult[0];
            if (hotSpare.ActivationMode === "Activated") {
                // override environment colors with hot spare
                shell.activateHotSpareEnvironment(hotSpare);
            } else {
                var envColor = (<any>doc)["EnvironmentColor"];
                if (envColor != null) {
                    shell.selectedEnvironmentColorStatic(new environmentColor(envColor.Name, envColor.BackgroundColor));
                }
            }
        }).fail(() => shell.selectedEnvironmentColorStatic(shell.originalEnvironmentColor()));
    }

    private activateDatabase(db: database) {
        if (db == null) {
            this.disconnectFromCurrentResource();
            return;
        }

        this.fecthStudioConfigForDatabase(db);

        var changeSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllDocs(() => this.fetchDbStats(db)),
            changesContext.currentResourceChangesApi().watchAllIndexes(() => this.fetchDbStats(db)),
            changesContext.currentResourceChangesApi().watchBulks(() => this.fetchDbStats(db))
        ];
        var isNotADatabase = this.currentConnectedResource instanceof database === false;
        this.updateChangesApi(db, isNotADatabase, () => this.fetchDbStats(db), changeSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof database && r.name === db.name));
    }

    private fetchDbStats(db: database) {
        if (!!db && !db.disabled() && db.isLicensed()) {
            new getDatabaseStatsCommand(db, true)
                .execute()
                .done((result: databaseStatisticsDto) => db.saveStatistics(result));
        }
    }

    private activateFileSystem(fs: fileSystem) {
        if (fs == null) {
            this.disconnectFromCurrentResource();
            return;
        }

        this.fecthStudioConfigForDatabase(new database(fs.name));

        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchFsFolders("", () => this.fetchFsStats(fs))
        ];
        var isNotAFileSystem = this.currentConnectedResource instanceof fileSystem === false;
        this.updateChangesApi(fs, isNotAFileSystem, () => this.fetchFsStats(fs), changesSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof fileSystem && r.name === fs.name));
    }

    private fetchFsStats(fs: fileSystem) {
        if (!!fs && !fs.disabled() && fs.isLicensed()) {
            new getFileSystemStatsCommand(fs, true)
                .execute()
                .done((result: filesystemStatisticsDto) => fs.saveStatistics(result))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get file system stats", response.responseText, response.statusText));
        }
    }

    private disconnectFromCurrentResource() {
        shell.disconnectFromResourceChangesApi();
        this.lastActivatedResource(null);
        this.currentConnectedResource = null; //TODO
    }

    private activateCounterStorage(cs: counterStorage) {
        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllCounters(() => this.fetchCsStats(cs)),
            changesContext.currentResourceChangesApi().watchCounterBulkOperation(() => this.fetchCsStats(cs))
        ];
        var isNotACounterStorage = this.currentConnectedResource instanceof counterStorage === false;
        this.updateChangesApi(cs, isNotACounterStorage, () => this.fetchCsStats(cs), changesSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof counterStorage && r.name === cs.name));
    }

    private fetchCsStats(cs: counterStorage) {
        if (!!cs && !cs.disabled() && cs.isLicensed()) {
            new getCounterStorageStatsCommand(cs, true)
                .execute()
                .done((result: counterStorageStatisticsDto) => cs.saveStatistics(result));
        }
    }

    private activateTimeSeries(ts: timeSeries) {
        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllTimeSeries(() => this.fetchTsStats(ts)),
            changesContext.currentResourceChangesApi().watchTimeSeriesBulkOperation(() => this.fetchTsStats(ts))
        ];
        var isNotATimeSeries = this.currentConnectedResource instanceof timeSeries === false;
        this.updateChangesApi(ts, isNotATimeSeries, () => this.fetchTsStats(ts), changesSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof timeSeries && r.name === ts.name));
    }

    private fetchTsStats(ts: timeSeries) {
        if (!!ts && !ts.disabled() && ts.isLicensed()) {
            new getTimeSeriesStatsCommand(ts, true)
                .execute()
                .done((result: timeSeriesStatisticsDto) => ts.saveStatistics(result));
        }
    }

    private updateChangesApi(rs: resource, isPreviousDifferentKind: boolean, fetchStats: () => void, subscriptionsArray: () => changeSubscription[]) {
        fetchStats();

        if (isPreviousDifferentKind || this.currentConnectedResource.name !== rs.name) {
            // disconnect from the current resource changes api and set the current connected resource
            shell.disconnectFromResourceChangesApi();
            this.currentConnectedResource = rs;
        }

        /*TODO:
        if ((!rs.disabled() && rs.isLicensed()) &&
            (isPreviousDifferentKind || changesContext.currentResourceChangesApi() == null)) {
            // connect to changes api, if it's not disabled and the changes api isn't already connected
            var changes = new changesApi(rs, 5000);
            changes.connectToChangesApiTask.done(() => {
                fetchStats();
                changesContext.currentResourceChangesApi(changes);
                shell.changeSubscriptionArray = subscriptionsArray();
            });
        }*/
    }

    setupApiKey() {
        // try to find api key as studio hash parameter
        var hash = window.location.hash;
        if (hash === "#has-api-key") {
            return this.showApiKeyDialog();
        } else if (hash.match(/#api-key/g)) {
            var match = /#api-key=(.*)/.exec(hash);
            if (match && match.length === 2) {
                oauthContext.apiKey(match[1]);
                apiKeyLocalStorage.setValue(match[1]);
            }
            var splittedHash = hash.split("&#api-key");
            var url = (splittedHash.length === 1) ? "#resources" : splittedHash[0];
            window.location.href = url;
        } else {
            var apiKeyFromStorage = apiKeyLocalStorage.get();
            if (apiKeyFromStorage) {
                oauthContext.apiKey(apiKeyFromStorage);
            }
        }

        oauthContext.authHeader.subscribe(h => {
            if (this.renewOAuthTokenTimeoutId) {
                clearTimeout(this.renewOAuthTokenTimeoutId);
                this.renewOAuthTokenTimeoutId = null;
            }
            if (h) {
                this.renewOAuthTokenTimeoutId = setTimeout(() => this.renewOAuthToken(), 25 * 60 * 1000);
            }
        });

        return $.Deferred().resolve();
    }

    private renewOAuthToken() {
        /* TODO: oauthContext.authHeader(null);
         new getDatabaseStatsCommand(null).execute();*/
    }

    showNavigationProgress(isNavigating: boolean) {
        if (isNavigating) {
            NProgress.start();

            var currentProgress = parseFloat(NProgress.status);
            var newProgress = isNaN(currentProgress) ? 0.5 : currentProgress + (currentProgress / 2);
            NProgress.set(newProgress);
        } else {
            NProgress.done();
        }
    }

    static reloadDatabases = () => shell.reloadResources(() => new getDatabasesCommand().execute(), shell.databases);
    static reloadFileSystems = () => shell.reloadResources(() => new getFileSystemsCommand().execute(), shell.fileSystems);
    static reloadCounterStorages = () => shell.reloadResources(() => new getCounterStoragesCommand().execute(), shell.counterStorages);
    static reloadTimeSeries = () => shell.reloadResources(() => new getTimeSeriesCommand().execute(), shell.timeSeries);

    private static reloadResources(getResources: () => JQueryPromise<any>, resourceObservableArray: KnockoutObservableArray<any>) {
        return getResources()
            .done((results) => this.updateResourceObservableArray(resourceObservableArray, results));
    }

    private static updateResourceObservableArray(resourceObservableArray: KnockoutObservableArray<any>, recievedResourceArray: Array<any>) {

        var deletedResources: Array<resource> = [];

        resourceObservableArray().forEach((rs: resource) => {
            var existingResource = recievedResourceArray.first((recievedResource: resource) => recievedResource.name === rs.name);
            if (existingResource == null) {
                deletedResources.push(rs);
            }
        });

        resourceObservableArray.removeAll(deletedResources);

        recievedResourceArray.forEach((recievedResource: resource) => {
            var foundResource: resource = resourceObservableArray().first((rs: resource) => rs.name === recievedResource.name);
            if (foundResource == null) {
                resourceObservableArray.push(recievedResource);
            } else {
                foundResource.disabled(recievedResource.disabled());
            }
        });
    }

    private reloadDataAfterReconnection(rs: resource) {
        //TODO: shell.fetchStudioConfig();
        //this.fetchServerBuildVersion();
        //this.fetchClientBuildVersion();
        //TODO: shell.fetchLicenseStatus();
        //this.fetchSupportCoverage();
        this.loadServerConfig();

        var databasesLoadTask = shell.reloadDatabases();
        var fileSystemsLoadTask = shell.reloadFileSystems();
        var counterStoragesLoadTask = shell.reloadCounterStorages();
        var timeSeriesLoadTask = shell.reloadTimeSeries();

        $.when(databasesLoadTask, fileSystemsLoadTask, counterStoragesLoadTask, timeSeriesLoadTask)
            .done(() => {
                var connectedResource = this.currentConnectedResource;
                var resourceObservableArray: any = shell.databases;
                var activeResourceObservable: any = this.activeDatabase;
                var isNotDatabase = !(connectedResource instanceof database);
                if (isNotDatabase && connectedResource instanceof fileSystem) {
                    resourceObservableArray = shell.fileSystems;
                    activeResourceObservable = this.activeFilesystem;
                }
                else if (isNotDatabase && connectedResource instanceof counterStorage) {
                    resourceObservableArray = shell.counterStorages;
                    activeResourceObservable = this.activeCounterStorage;
                }
                else if (isNotDatabase && connectedResource instanceof timeSeries) {
                    resourceObservableArray = shell.timeSeries;
                    activeResourceObservable = this.activeTimeSeries;
                }
                this.selectNewActiveResourceIfNeeded(resourceObservableArray, activeResourceObservable);
            });
    }

    private selectNewActiveResourceIfNeeded(resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any) {
        var activeResource = activeResourceObservable();
        var actualResourceObservableArray = resourceObservableArray();

        if (!!activeResource && actualResourceObservableArray.contains(activeResource) === false) {
            if (actualResourceObservableArray.length > 0) {
                resourceObservableArray().first().activate();
            } else { //if (actualResourceObservableArray.length == 0)
                shell.disconnectFromResourceChangesApi();
                activeResourceObservable(null);
                this.lastActivatedResource(null);
            }

            this.navigate(appUrl.forResources());
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            this.globalChangesApi.watchDocsStartingWith("Raven/Databases/", (e) => this.changesApiFiredForResource(e, shell.databases, this.activeDatabase, TenantType.Database)),
            this.globalChangesApi.watchDocsStartingWith("Raven/FileSystems/", (e) => this.changesApiFiredForResource(e, shell.fileSystems, this.activeFilesystem, TenantType.FileSystem)),
            this.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForResource(e, shell.counterStorages, this.activeCounterStorage, TenantType.CounterStorage)),
            this.globalChangesApi.watchDocsStartingWith("Raven/TimeSeries/", (e) => this.changesApiFiredForResource(e, shell.timeSeries, this.activeTimeSeries, TenantType.TimeSeries)),
            //TODO: this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => shell.fetchStudioConfig()),
            this.globalChangesApi.watchDocsStartingWith("Raven/Alerts", () => this.fetchSystemDatabaseAlerts())
        ];
    }

    private changesApiFiredForResource(e: documentChangeNotificationDto,
        resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any, resourceType: TenantType) {

        if (!!e.Id && (e.Type === "Delete" || e.Type === "Put")) {
            var receivedResourceName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === "Delete") {
                var resourceToDelete = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);
                if (!!resourceToDelete) {
                    resourceObservableArray.remove(resourceToDelete);

                    //this.selectNewActiveResourceIfNeeded(resourceObservableArray, activeResourceObservable);
                    if (resourceType == TenantType.Database)
                        recentQueriesStorage.removeRecentQueries(resourceToDelete);
                }
            } else { // e.Type === "Put"
                var getSystemDocumentTask = new getSystemDocumentCommand(e.Id).execute();
                getSystemDocumentTask.done((dto: databaseDocumentDto) => {
                    var existingResource = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);

                    if (existingResource == null) { // new resource
                        existingResource = this.createNewResource(resourceType, receivedResourceName, dto);
                        resourceObservableArray.unshift(existingResource);
                    } else {
                        if (existingResource.disabled() != dto.Disabled) { //disable status change
                            existingResource.disabled(dto.Disabled);
                            if (dto.Disabled === false && this.currentConnectedResource.name === receivedResourceName) {
                                existingResource.activate();
                            }
                        }
                    }

                    if (resourceType === TenantType.Database) { //for databases, bundle change or indexing change
                        var dtoSettings: any = dto.Settings;
                        var bundles = !!dtoSettings["Raven/ActiveBundles"] ? dtoSettings["Raven/ActiveBundles"].split(";") : [];
                        existingResource.activeBundles(bundles);


                        var indexingDisabled = this.getIndexingDisbaledValue(dtoSettings["Raven/IndexingDisabled"]);
                        existingResource.indexingDisabled(indexingDisabled);

                        var isRejectclientsEnabled = this.getIndexingDisbaledValue(dtoSettings["Raven/RejectClientsModeEnabled"]);
                        existingResource.rejectClientsMode(isRejectclientsEnabled);
                    }
                });
            }
        }
    }

    private getIndexingDisbaledValue(indexingDisabledString: string) {
        if (indexingDisabledString === undefined || indexingDisabledString == null)
            return false;

        if (indexingDisabledString.toLowerCase() === "true")
            return true;

        return false;
    }

    private createNewResource(resourceType: TenantType, resourceName: string, dto: databaseDocumentDto): resource {
        var newResource: resource = null;

        if (resourceType === TenantType.Database) {
            newResource = new database(resourceName, true, dto.Disabled);
        }
        else if (resourceType === TenantType.FileSystem) {
            newResource = new fileSystem(resourceName, true, dto.Disabled);
        }
        else if (resourceType === TenantType.CounterStorage) {
            newResource = new counterStorage(resourceName, true, dto.Disabled);
        }
        else if (resourceType === TenantType.TimeSeries) {
            newResource = new timeSeries(resourceName, true, dto.Disabled);
        }

        return newResource;
    }

    selectResource(rs: resource) {
        rs.activate();

        var locationHash = window.location.hash;
        var isMainPage = locationHash === appUrl.forResources();
        if (isMainPage === false) {
            var updatedUrl = appUrl.forCurrentPage(rs);
            this.navigate(updatedUrl);
        }
    }

    private databasesLoaded(databases: database[]) {
        shell.databases(databases);
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    private loadDatabases(): JQueryPromise<any> {
        var deferred = $.Deferred();

        this.databasesLoadedTask = new getDatabasesCommand()
            .execute()
            .fail(result => this.handleRavenConnectionFailure(result))
            .done((results: database[]) => {
                this.databasesLoaded(results);
                //TODO: shell.fetchStudioConfig();
                //TODO: this.fetchClusterTopology();
                //TODO: this.fetchServerBuildVersion();
                //TODO: this.fetchClientBuildVersion();
                //TODO:shell.fetchLicenseStatus();
                //TODO: this.fetchSupportCoverage();
                //TODO :this.fetchSystemDatabaseAlerts();
                router.activate();
            })
            .always(() => deferred.resolve());

        return deferred;
    }

    private loadFileSystems(): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getFileSystemsCommand()
            .execute()
            .done((results: fileSystem[]) => shell.fileSystems(results))
            .always(() => deferred.resolve());

        return deferred;
    }

    private loadCounterStorages(): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => shell.counterStorages(results))
            .always(() => deferred.resolve());

        return deferred;
    }

    private loadTimeSeries(): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getTimeSeriesCommand()
            .execute()
            .done((results: timeSeries[]) => shell.timeSeries(results))
            .always(() => deferred.resolve());

        return deferred;
    }

    loadServerConfig() {
        var deferred = $.Deferred();

        new getServerConfigsCommand()
            .execute()
            .done((serverConfigs: serverConfigsDto) => {
                accessHelper.isGlobalAdmin(serverConfigs.IsGlobalAdmin);
                accessHelper.canReadWriteSettings(serverConfigs.CanReadWriteSettings);
                accessHelper.canReadSettings(serverConfigs.CanReadSettings);
                accessHelper.canExposeConfigOverTheWire(serverConfigs.CanExposeConfigOverTheWire);
            })
            .always(() => deferred.resolve());

        return deferred;
    }

    connectToRavenServer() {
        var serverConfigsLoadTask: JQueryPromise<any> = this.loadServerConfig();
        var databasesLoadTask: JQueryPromise<any> = this.loadDatabases();
        var fileSystemsLoadTask: JQueryPromise<any> = this.loadFileSystems();
        var counterStoragesLoadTask: JQueryPromise<any> = this.loadCounterStorages();
        var timeSeriesLoadTask: JQueryPromise<any> = this.loadTimeSeries();
        $.when(serverConfigsLoadTask, databasesLoadTask, fileSystemsLoadTask, counterStoragesLoadTask, timeSeriesLoadTask)
            .always(() => {
                var locationHash = window.location.hash;
                if (appUrl.getFileSystem()) { //filesystems section
                    this.activateResource(appUrl.getFileSystem(), shell.fileSystems, appUrl.forResources);
                } else if (appUrl.getCounterStorage()) { //counter storages section
                    this.activateResource(appUrl.getCounterStorage(), shell.counterStorages, appUrl.forResources);
                }
                else if (appUrl.getTimeSeries()) { //time series section
                    this.activateResource(appUrl.getTimeSeries(), shell.timeSeries, appUrl.forResources);
                }
                else if ((locationHash.indexOf(appUrl.forAdminSettings()) === -1)) { //databases section
                    this.activateResource(appUrl.getDatabase(), shell.databases, appUrl.forResources);
                }
            });
    }

    private activateResource(resource: resource, resourceObservableArray: KnockoutObservableArray<any>, url: () => string) {
        if (!!resource) {
            var newResource = resourceObservableArray.first(rs => rs.name === resource.name);
            if (newResource != null) {
                newResource.activate();
            } else {
                messagePublisher.reportError("The resource " + resource.name + " doesn't exist!");
                this.navigate(url());
            }
        }
    }

    navigateToResources() {
        shell.disconnectFromResourceChangesApi();

        if (!!this.activeDatabase()) {
            shell.databases().length === 1 ? this.activeDatabase(null) : this.activeDatabase().activate();
        } else if (!!this.activeFilesystem()) {
            this.activeFilesystem().activate();
        } else if (!!this.activeCounterStorage()) {
            this.activeCounterStorage().activate();
        } else if (!!this.activeTimeSeries()) {
            this.activeTimeSeries().activate();
        }

        this.navigate(appUrl.forResources());
    }

    private static activateHotSpareEnvironment(hotSpare: HotSpareDto) {
        var color = new environmentColor(hotSpare.ActivationMode === "Activated" ? "Active Hot Spare" : "Hot Spare", "#FF8585");
        license.hotSpare(hotSpare);
        shell.selectedEnvironmentColorStatic(color);
        shell.originalEnvironmentColor(color);
    }

    private handleRavenConnectionFailure(result: any) {
        NProgress.done();

        if (result.status === 401) {
            // Unauthorized might be caused by invalid credentials. 
            // Remove them from both local storage and oauth context.
            apiKeyLocalStorage.clean();
            oauthContext.clean();
        }

        sys.log("Unable to connect to Raven.", result);
        var tryAgain = "Try again";
        var messageBoxResultPromise = this.confirmationMessage(':-(', "Couldn't connect to Raven. Details in the browser console.", [tryAgain]);
        messageBoxResultPromise.done(() => {
            NProgress.start();
            this.connectToRavenServer();
        });
    }

    dataLoadProgress(splashType?: alertType) {
        if (!splashType) {
            NProgress.configure({ showSpinner: false });
            NProgress.done();
        } else if (splashType === alertType.warning) {
            NProgress.configure({ showSpinner: true });
            NProgress.start();
        } else {
            NProgress.done();
            NProgress.configure({ showSpinner: false });
            $.blockUI({ message: '<div id="longTimeoutMessage"><span> This is taking longer than usual</span><br/><span>(Waiting for server to respond)</span></div>' });
        }
    }

    showAlert(alert: alertArgs) {
        if (alert.displayInRecentErrors && (alert.type === alertType.danger || alert.type === alertType.warning)) {
            this.recordedErrors.unshift(alert);
        }

        var currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlert = alert;
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);
            var fadeTime = 2000; // If there are no pending alerts, show it for 2 seconds before fading out.
            /*            if (alert.title.indexOf("Changes stream was disconnected.") == 0) {
                            fadeTime = 100000000;
                        }*/
            if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 5000; // If there are pending alerts, show the error alert for 4 seconds before fading out.
            }
            setTimeout(() => {
                this.closeAlertAndShowNext(alert);
            }, fadeTime);
        }
    }

    closeAlertAndShowNext(alertToClose: alertArgs) {
        var alertElement = $('#' + alertToClose.id);
        if (alertElement.length === 0) {
            return;
        }

        // If the mouse is over the alert, keep it around.
        if (alertElement.is(":hover")) {
            setTimeout(() => this.closeAlertAndShowNext(alertToClose), 1000);
        } else {
            alertElement.alert("close");
        }
    }

    onAlertHidden() {
        this.currentAlert(null);
        var nextAlert = this.queuedAlert;
        if (nextAlert) {
            this.queuedAlert = null;
            this.showAlert(nextAlert);
        }
    }

    private activateDatabaseWithName(databaseName: string) {
        if (this.databasesLoadedTask) {
            this.databasesLoadedTask.done(() => {
                var matchingDatabase = shell.databases().first(db => db.name == databaseName);
                if (matchingDatabase && this.activeDatabase() !== matchingDatabase) {
                    ko.postbox.publish("ActivateDatabase", matchingDatabase);
                }
            });
        }
    }

    static disconnectFromResourceChangesApi() {
        if (changesContext.currentResourceChangesApi()) {
            shell.changeSubscriptionArray.forEach((subscripbtion: changeSubscription) => subscripbtion.off());
            shell.changeSubscriptionArray = [];
            changesContext.currentResourceChangesApi().dispose();
            changesContext.currentResourceChangesApi(null);
        }
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass((<any>doc)["@metadata"]["Raven-Entity-Name"], this.activeDatabase());
    }

    fetchServerBuildVersion() {
        /* TODO:
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                this.serverBuildVersion(serverBuildResult);

                var currentBuildVersion = serverBuildResult.BuildVersion;
                if (currentBuildVersion !== 13) {
                    shell.serverMainVersion(Math.floor(currentBuildVersion / 10000));
                }

                if (serverBuildReminder.isReminderNeeded() && currentBuildVersion !== 13) {
                    new getLatestServerBuildVersionCommand(true, 35000, 39999) //pass false as a parameter to get the latest unstable
                        .execute()
                        .done((latestServerBuildResult: latestServerBuildVersionDto) => {
                            if (latestServerBuildResult.LatestBuild > currentBuildVersion) { //
                                var latestBuildReminderViewModel = new latestBuildReminder(latestServerBuildResult);
                                app.showDialog(latestBuildReminderViewModel);
                            }
                        });
                }
            });*/
    }

    fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => { this.clientBuildVersion(result); });
    }

    fetchClusterTopology() {
        new getClusterTopologyCommand(null)
            .execute()
            .done((topology: topology) => {
                shell.clusterMode(topology && topology.allNodes().length > 0);
            });
    }

    static fetchLicenseStatus(): JQueryPromise<licenseStatusDto> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
                if (result.Status.contains("AGPL")) {
                    result.Status = "Development Only";
                }
                license.licenseStatus(result);
            });
    }

    fetchSupportCoverage() {
        new getSupportCoverageCommand()
            .execute()
            .done((result: supportCoverageDto) => {
                license.supportCoverage(result);
            });
    }

    showApiKeyDialog() {
        var dialog = new enterApiKey();
        return app.showDialog(dialog).then(() => window.location.href = "#resources");
    }

    showErrorsDialog() {
        var errorDetails: recentErrors = new recentErrors(this.recordedErrors);
        app.showDialog(errorDetails);
    }

    uploadStatusChanged(item: uploadItem) {
        var queue: uploadItem[] = uploadQueueHelper.parseUploadQueue(window.localStorage[uploadQueueHelper.localStorageUploadQueueKey + item.filesystem.name], item.filesystem);
        uploadQueueHelper.updateQueueStatus(item.id(), item.status(), queue);
        uploadQueueHelper.updateLocalStorage(queue, item.filesystem);
    }

    showLicenseStatusDialog() {
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
        app.showDialog(dialog);
    }

    fetchSystemDatabaseAlerts() {
        /* TODO
        new getDocumentWithMetadataCommand("Raven/Alerts", this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                //
            });*/
    }

    logOut() {
        // this call dispatches storage event to all tabs in browser and calls onLogOut inside each.
        apiKeyLocalStorage.clean();
        apiKeyLocalStorage.notifyAboutLogOut();
    }

    onLogOut() {
        window.location.hash = this.appUrls.hasApiKey();
        window.location.reload();
    }

    navigateToClusterSettings() {
        this.navigate(this.appUrls.adminSettingsCluster());
    }

    selectNone() {
        this.activateDatabase(null);
        this.activeFilesystem(null);
    }

    private spinnerOptions = {
        lines: 17, // The number of lines to draw
        length: 28, // The length of each line
        width: 14, // The line thickness
        radius: 44, // The radius of the inner circle
        scale: 1, // Scales overall size of the spinner
        corners: 1, // Corner roundness (0..1)
        color: ["#d74c0c", "#CC0000"], // #rgb or #rrggbb or array of colors
        opacity: 0.35, // Opacity of the lines
        rotate: 0, // The rotation offset
        direction: 1, // 1: clockwise, -1: counterclockwise
        speed: 0.8, // Rounds per second
        trail: 60, // Afterglow percentage
        fps: 20, // Frames per second when using setTimeout() as a fallback for CSS
        zIndex: 2e9, // The z-index (defaults to 2000000000)
        className: "spinner", // The CSS class to assign to the spinner
        top: "50%", // Top position relative to parent
        left: "50%", // Left position relative to parent
        shadow: false, // Whether to render a shadow
        hwaccel: false, // Whether to use hardware acceleration
        position: "absolute" // Element positioning
    };
    private spinner = new Spinner(this.spinnerOptions);

    static resourcesNamesComputed(): KnockoutComputed<string[]> {
        return ko.computed(() => {
            var resourcesNames = shell.resources().map((rs: resource) => rs.name);
            return resourcesNames.distinct();
        });
    }
}

export = shell;
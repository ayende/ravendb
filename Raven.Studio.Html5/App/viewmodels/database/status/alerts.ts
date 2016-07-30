import router = require("plugins/router"); 
import getAlertsCommand = require("commands/operations/getAlertsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import alert = require("models/database/debug/alert");
import appUrl = require("common/appUrl");
import saveAlertsCommand = require("commands/operations/saveAlertsCommand");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");
import tableNavigationTrait = require("common/tableNavigationTrait");

class alerts extends viewModelBase {

    alertDoc = ko.observable<alertContainerDto>();
    allAlerts = ko.observableArray<alert>();
    filterLevel = ko.observable("All");
    selectedAlert = ko.observable<alert>();
    selectedAlertIndex = ko.observable<number>();
    unreadAlertCount: KnockoutComputed<number>;
    readAlertCount: KnockoutComputed<number>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    isSaveEnabled: KnockoutComputed<boolean>;

    autoRefreshEnabled = ko.observable<boolean>(true);
    tableNavigation: tableNavigationTrait<alert>;

    constructor() {
        super();

        autoRefreshBindingHandler.install();

        this.unreadAlertCount = ko.computed(() => this.allAlerts().count(a => !a.observed()));
        this.readAlertCount = ko.computed(() => this.allAlerts().count(a => a.observed()));
        this.updateCurrentNowTime();
        this.activeDatabase.subscribe(() => this.fetchAlerts());

        this.tableNavigation = new tableNavigationTrait<alert>("#alertTableContainer", this.selectedAlert, this.allAlerts, i => "#alertsContainer table tbody tr:nth-child(" + (i + 1) + ")");
    }

    activate(args) {
        super.activate(args);
        this.fetchAlerts();
        var item = !!args.item && !isNaN(args.item) ? args.item : 0;
        this.updateHelpLink('HL46QE');
        this.selectedAlertIndex(item);

        this.dirtyFlag = new ko.DirtyFlag([this.allAlerts]);
        this.isSaveEnabled = ko.computed(() => {
            var refresh = this.autoRefreshEnabled();
            var isDirty = this.dirtyFlag().isDirty();
            return !refresh && isDirty;
        });
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
    }

    fetchAlerts(): JQueryPromise<alertContainerDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getAlertsCommand(db)
                .execute()
                .done((result: alertContainerDto) => this.processAlertsResults(result));
        }

        return null;
    }

    processAlertsResults(result: alertContainerDto) {
        var alerts = result.Alerts.map(a => new alert(a));
        alerts.forEach(r => {
            r.createdAtHumanized = this.createHumanReadableTime(r.createdAt),
            r.isVisible = ko.computed(() => this.matchesFilter(r));
        });
        this.alertDoc(result);
        this.allAlerts(alerts);
        if (alerts.length > 0) {
            this.selectAlert(alerts[this.selectedAlertIndex()]);
        }
        this.dirtyFlag().reset();
    }

    matchesFilter(a: alert): boolean {
        if (this.filterLevel() === "All") {
            return true;
        }

        var unreadFilterWithUnreadAlert = this.filterLevel() === "Unread" && !a.observed();
        var readFilterWithReadAlert = this.filterLevel() === "Read" && a.observed();
        return unreadFilterWithUnreadAlert || readFilterWithReadAlert;
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now (scheduled to occur every minute.)
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }

    selectAlert(selection: alert) {
        var index = this.allAlerts.indexOf(selection);
        this.selectedAlertIndex(index);
        this.selectedAlert(selection);

        var alertUrl = appUrl.forAlerts(this.activeDatabase()) + "&item=" + this.selectedAlertIndex();
        router.navigate(alertUrl, false);
    }

    setFilterAll() {
        this.filterLevel("All");
    }

    setFilterUnread() {
        this.filterLevel("Unread");
    }

    setFilterRead() {
        this.filterLevel("Read");
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    toggleSelectedReadState() {
        this.disableAutoRefresh(); 
        var alert = this.selectedAlert();
        if (alert) {
            if (!alert.observed()) {
                alert.lastDismissedAt = this.now().toISOString();
            }
            alert.observed(!alert.observed());
        }
    }

    private disableAutoRefresh() {
        this.autoRefreshEnabled(false);
    }

    deleteSelectedAlert() {
        this.disableAutoRefresh();
        var alert = this.selectedAlert();
        if (alert) {
            this.allAlerts.remove(alert);
        }
    }

    deleteReadAlerts() {
        this.disableAutoRefresh();
        this.allAlerts.remove(a => a.observed());
    }

    deleteAllAlerts() {
        this.disableAutoRefresh();
        this.allAlerts.removeAll();
    }

    saveAlerts() {
        var alertDoc = this.alertDoc();
        var db = this.activeDatabase();
        if (alertDoc && db) {
            alertDoc.Alerts = this.allAlerts().map(a => a.toDto());
            new saveAlertsCommand(alertDoc, db)
                .execute()
                .done(() => this.fetchAlerts());

        }
    }
}

export = alerts;

﻿import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import startDbCompactCommand = require("commands/maintenance/startCompactCommand");
import startFsCompactCommand = require("commands/filesystem/startCompactCommand");

class resourceCompact {
    resourceName = ko.observable<string>('');
    
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    compactStatusMessages = ko.observableArray<string>();
    compactStatusLastUpdate = ko.observable<string>();

    keepDown = ko.observable<boolean>(false);

    constructor(private parent: compact, private type: string, private resources: KnockoutObservableArray<resource>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName == rs.name);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = (this.type == database.type ? "Database" : "File system") + " name doesn't exist!";
            }

            return errorMessage;
        });
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown() == true) {
            var logsPre = document.getElementById(this.type + 'CompactLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
    }

    updateCompactStatus(newCompactStatus: compactStatusDto) {
        this.compactStatusMessages(newCompactStatus.Messages);
        this.compactStatusLastUpdate(newCompactStatus.LastProgressMessage);
        if (this.keepDown()) {
            var logsPre = document.getElementById(this.type + 'CompactLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
        this.parent.isBusy(newCompactStatus.State == "Running");
    }

}
class compact extends viewModelBase {
    private dbCompactOptions = new resourceCompact(this, database.type, shell.databases);
    private fsCompactOptions = new resourceCompact(this, filesystem.type, shell.fileSystems);

    isBusy = ko.observable<boolean>();

    canActivate(args): any {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('7HZGOE');
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which != 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which != 13);
    }

    startDbCompact() {
        this.isBusy(true);
        var self = this;

        new startDbCompactCommand(this.dbCompactOptions.resourceName(), self.dbCompactOptions.updateCompactStatus.bind(self.dbCompactOptions))
            .execute();
    }

    startFsCompact() {
        this.isBusy(true);
        var self = this;

        new startFsCompactCommand(this.fsCompactOptions.resourceName(), self.fsCompactOptions.updateCompactStatus.bind(self.fsCompactOptions))
            .execute();
    }
}

export = compact;
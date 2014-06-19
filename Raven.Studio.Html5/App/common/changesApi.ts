/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import resource = require('models/resource');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
import folder = require("models/filesystem/folder");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");

class changesApi {

    private eventsId: string;
    private webSocket: WebSocket;
    private isConnectionClosed: boolean = false;

    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<transformerChangeNotificationDto>>();
    private watchedPrefixes = {};
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private allFsSyncHandlers = ko.observableArray<changesCallback<synchronizationUpdateNotification>>();
    private allFsConflictsHandlers = ko.observableArray<changesCallback<synchronizationConflictNotification>>();
    private watchedFolders = {};
    private commandBase = new commandBase();

    constructor(private rs: resource, coolDownWithDataLoss?:number) {
        this.eventsId = this.makeId();
        this.connect(coolDownWithDataLoss);
    }

    private connect(coolDownWithDataLoss: number = 0) {
        if ("WebSocket" in window) {
            var host = window.location.host;
            var resourceUrl = appUrl.forResourceQuery(this.rs);

            console.log("Connecting to changes API (rs = " + this.rs.name + ")");

            var getTokenTask = new getSingleAuthTokenCommand(this.rs).execute();
            getTokenTask.done((tokenObject: singleAuthToken) => {
                var token = tokenObject.Token;

                this.webSocket = new WebSocket('ws://' + host + resourceUrl + '/changes/websocket?singleUseAuthToken=' + token + '&id=' + this.eventsId + '&coolDownWithDataLoss=' + coolDownWithDataLoss);

                this.webSocket.onmessage = (e) => this.onEvent(e);
                this.webSocket.onerror = (e) => this.onError(e);
                this.webSocket.onclose = (e) => this.isConnectionClosed = true;
            });
        }
        else {
            console.log("WebSocket NOT supported by your Browser!"); // The browser doesn't support WebSocket
        }
    }

    private send(command: string, value?: string) {
        var args = {
            id: this.eventsId,
            command: command
        };
        if (value !== undefined) {
            args["value"] = value;
        }
        //TODO: exception handling?
        this.commandBase.query('/changes/config', args, this.rs);

    }

    private onError(e: any) {
        this.isConnectionClosed = true;
        this.commandBase.reportError('Changes stream was disconnected. Retrying connection shortly.');
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private onEvent(e: any) {
        var eventDto: changesApiEventDto = JSON.parse(e.data);
        var type = eventDto.Type;
        var value = eventDto.Value;

        if (type !== "Heartbeat") { // ignore heartbeat
            if (type === "DocumentChangeNotification") {
                this.fireEvents(this.allDocsHandlers(), value, (e) => true);
                for (var key in this.watchedPrefixes) {
                    var docCallbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedPrefixes[key];
                    this.fireEvents(docCallbacks(), value, (e) => e.Id != null && e.Id.match("^" + key));
                }
            } else if (type === "IndexChangeNotification") {
                this.fireEvents(this.allIndexesHandlers(), value, (e) => true);
            } else if (type === "TransformerChangeNotification") {
                this.fireEvents(this.allTransformersHandlers(), value, (e) => true);
            } else if (type === "BulkInsertChangeNotification") {
                this.fireEvents(this.allBulkInsertsHandlers(), value, (e) => true);
            } else if (type === "SynchronizationUpdateNotification") {
                this.fireEvents(this.allFsSyncHandlers(), value, (e) => true);
            } else if (type === "ConflictNotification") {
                this.fireEvents(this.allFsConflictsHandlers(), value, (e) => true);
            } else if (type == "FileChangeNotification") {
                for (var key in this.watchedFolders) {
                    var folderCallbacks = <KnockoutObservableArray<fileChangeNotification>> this.watchedFolders[key];
                    this.fireEvents(folderCallbacks(), value, (e) => {
                        var notifiedFolder = folder.getFolderFromFilePath(e.File);
                        var match: string[] = null
                        if (notifiedFolder && notifiedFolder.path) {
                            match = notifiedFolder.path.match(key);
                        }
                        return match && match.length > 0;
                    });
                }
            } else {
                console.log("Unhandled Changes API notification type: " + type);
            }
        }
    }

    watchAllIndexes(onChange: (e: indexChangeNotificationDto) => void) {
        var callback = new changesCallback<indexChangeNotificationDto>(onChange);
        if (this.allIndexesHandlers().length == 0) {
            this.send('watch-indexes');
        }
        this.allIndexesHandlers.push(callback);
        return new changeSubscription(() => {
            this.allIndexesHandlers.remove(callback);
            if (this.allIndexesHandlers().length == 0) {
                this.send('unwatch-indexes');
            }
        });
    }

    watchAllTransformers(onChange: (e: transformerChangeNotificationDto) => void) {
        var callback = new changesCallback<transformerChangeNotificationDto>(onChange);
        if (this.allTransformersHandlers().length == 0) {
            this.send('watch-transformers');
        }
        this.allTransformersHandlers.push(callback);
        return new changeSubscription(() => {
            this.allTransformersHandlers.remove(callback);
            if (this.allTransformersHandlers().length == 0) {
                this.send('unwatch-transformers');
            }
        });
    }

    watchAllDocs(onChange: (e: documentChangeNotificationDto) => void) {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (this.allDocsHandlers().length == 0) {
            this.send('watch-docs');
        }
        this.allDocsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-docs');
            }
        });
    }

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: documentChangeNotificationDto) => void): changeSubscription {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (typeof (this.watchedPrefixes[docIdPrefix]) === "undefined") {
            this.send('watch-prefix', docIdPrefix);
            this.watchedPrefixes[docIdPrefix] = ko.observableArray();
        }
        this.watchedPrefixes[docIdPrefix].push(callback);

        return new changeSubscription(() => {
            this.watchedPrefixes[docIdPrefix].remove(callback);
            if (this.watchedPrefixes[docIdPrefix]().length == 0) {
                delete this.watchedPrefixes[docIdPrefix];
                this.send('unwatch-prefix', docIdPrefix);
            }
        });
    }

    watchBulks(onChange: (e: bulkInsertChangeNotificationDto) => void) {
        var callback = new changesCallback<bulkInsertChangeNotificationDto>(onChange);
        if (this.allBulkInsertsHandlers().length == 0) {
            this.send('watch-bulk-operation');
        }
        this.allBulkInsertsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allBulkInsertsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-bulk-operation');
            }
        });
    }

    watchDocPrefix(onChange: (e: documentChangeNotificationDto) => void, prefix?: string) {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (this.allDocsHandlers().length == 0) {
            this.send('watch-prefix', prefix);
        }
        this.allDocsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-prefix', prefix);
            }
        });
    }

    watchFsSync(onChange: (e: synchronizationUpdateNotification) => void): changeSubscription {
        var callback = new changesCallback<synchronizationUpdateNotification>(onChange);
        if (this.allFsSyncHandlers().length == 0) {
            this.send('watch-sync');
        }
        this.allFsSyncHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsSyncHandlers.remove(callback);
            if (this.allFsSyncHandlers().length == 0) {
                this.send('unwatch-sync');
            }
        });
    }

    watchFsConflicts(onChange: (e: synchronizationConflictNotification) => void) : changeSubscription {
        var callback = new changesCallback<synchronizationConflictNotification>(onChange);
        if (this.allFsConflictsHandlers().length == 0) {
            this.send('watch-conflicts');
        }
        this.allFsConflictsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsConflictsHandlers.remove(callback);
            if (this.allFsConflictsHandlers().length == 0) {
                this.send('unwatch-conflicts');
            }
        });
    }

    watchFsFolders(folder: string, onChange: (e: fileChangeNotification) => void): changeSubscription {
        var callback = new changesCallback<fileChangeNotification>(onChange);
        if (typeof (this.watchedFolders[folder]) === "undefined") {
            this.send('watch-folder', folder);
            this.watchedFolders[folder] = ko.observableArray();
        }
        this.watchedFolders[folder].push(callback);
        return new changeSubscription(() => {
            this.watchedFolders[folder].remove(callback);
            if (this.watchedFolders[folder].length == 0) {
                delete this.watchedFolders[folder];
                this.send('unwatch-folder', folder);
            }
        });
    }

    dispose() {
        if (this.webSocket && !this.isConnectionClosed) {
            console.log("Disconnecting from changes API for " + this.rs.name);
            this.send('disconnect');
            this.webSocket.close();
        }
    }

    private makeId() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }

}

export = changesApi;
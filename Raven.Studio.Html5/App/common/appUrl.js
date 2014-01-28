define(["require", "exports", "models/database", "common/pagedList"], function(require, exports, database, pagedList) {
    // Helper class with static methods for generating app URLs.
    var appUrl = (function () {
        function appUrl() {
        }
        appUrl.forDatabases = function () {
            return "#databases";
        };

        /**
        * Gets the URL for edit document.
        * @param id The ID of the document to edit, or null to edit a new document.
        * @param collectionName The name of the collection to page through on the edit document, or null if paging will be disabled.
        * @param docIndexInCollection The 0-based index of the doc to edit inside the paged collection, or null if paging will be disabled.
        * @param database The database to use in the URL. If null, the current database will be used.
        */
        appUrl.forEditDoc = function (id, collectionName, docIndexInCollection, db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            var databaseUrlPart = appUrl.getEncodedDbPart(db);
            var docIdUrlPart = id ? "&id=" + encodeURIComponent(id) : "";
            var pagedListInfo = collectionName && docIndexInCollection != null ? "&list=" + encodeURIComponent(collectionName) + "&item=" + docIndexInCollection : "";
            return "#edit?" + docIdUrlPart + databaseUrlPart + pagedListInfo;
        };

        /**
        * Gets the URL for status page.
        * @param database The database to use in the URL. If null, the current database will be used.
        */
        appUrl.forStatus = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forSettings = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#settings?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forLogs = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status/logs?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forAlerts = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status/alerts?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forIndexErrors = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status/indexErrors?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forReplicationStats = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status/replicationStats?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forUserInfo = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            return "#status/userInfo?" + appUrl.getEncodedDbPart(db);
        };

        appUrl.forDocuments = function (collection, db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            var collectionPart = collection ? "collection=" + encodeURIComponent(collection) : "";
            var databasePart = appUrl.getEncodedDbPart(db);
            return "#documents?" + collectionPart + databasePart;
        };

        appUrl.forIndexes = function (db) {
            if (typeof db === "undefined") { db = appUrl.getDatabase(); }
            var databasePart = appUrl.getEncodedDbPart(db);
            return "#indexes?" + databasePart;
        };

        appUrl.forNewIndex = function (db) {
            var databasePart = appUrl.getEncodedDbPart(db);
            return "#indexes/edit?" + databasePart;
        };

        appUrl.forEditIndex = function (indexName, db) {
            var databasePart = appUrl.getEncodedDbPart(db);
            var indexNamePart = "/" + indexName;
            return "#indexes/edit" + indexNamePart + databasePart;
        };

        appUrl.forQuery = function (db) {
            var databasePart = appUrl.getEncodedDbPart(db);
            return "#query?" + databasePart;
        };

        appUrl.forDatabaseQuery = function (db) {
            if (db && !db.isSystem) {
                return appUrl.baseUrl + "/databases/" + db.name;
            }

            return this.baseUrl;
        };

        appUrl.forExport = function (db) {
            var databasePart = appUrl.getEncodedDbPart(db);
            return "#tasks/export?" + databasePart;
        };

        /**
        * Gets the database from the current web browser address. Returns the system database if no database name is found.
        */
        appUrl.getDatabase = function () {
            // TODO: instead of string parsing, can we pull this from durandal.activeInstruction()?
            var dbIndicator = "database=";
            var hash = window.location.hash;
            var dbIndex = hash.indexOf(dbIndicator);
            if (dbIndex >= 0) {
                // A database is specified in the address.
                var dbSegmentEnd = hash.indexOf("&", dbIndex);
                if (dbSegmentEnd === -1) {
                    dbSegmentEnd = hash.length;
                }

                var databaseName = hash.substring(dbIndex + dbIndicator.length, dbSegmentEnd);
                var unescapedDatabaseName = decodeURIComponent(databaseName);
                var db = new database(unescapedDatabaseName);
                db.isSystem = unescapedDatabaseName === "<system>";
                return db;
            } else {
                // No database is specified in the URL. Assume it's the system database.
                var db = new database("<system>");
                db.isSystem = true;
                return db;
            }
        };

        /**
        * Gets the server URL.
        */
        appUrl.forServer = function () {
            // Ported this code from old Silverlight Studio. Do we still need this?
            if (window.location.protocol === "file:") {
                if (window.location.search.indexOf("fiddler")) {
                    return "http://localhost.fiddler:8080";
                } else {
                    return "http://localhost:8080";
                }
            }

            return window.location.protocol + "//" + window.location.host;
        };

        /**
        * Gets an object containing computed URLs that update when the current database updates.
        */
        appUrl.forCurrentDatabase = function () {
            return appUrl.currentDbComputeds;
        };

        appUrl.getEncodedDbPart = function (db) {
            return db ? "&database=" + encodeURIComponent(db.name) : "";
        };
        appUrl.baseUrl = "http://localhost:8080";

        appUrl.currentDatabase = ko.observable().subscribeTo("ActivateDatabase", true);

        appUrl.currentDbComputeds = {
            documents: ko.computed(function () {
                return appUrl.forDocuments(null, appUrl.currentDatabase());
            }),
            indexes: ko.computed(function () {
                return appUrl.forIndexes(appUrl.currentDatabase());
            }),
            newIndex: ko.computed(function () {
                return appUrl.forNewIndex(appUrl.currentDatabase());
            }),
            editIndex: function (indexName) {
                return ko.computed(function () {
                    return appUrl.forEditIndex(indexName, appUrl.currentDatabase());
                });
            },
            query: ko.computed(function () {
                return appUrl.forQuery(appUrl.currentDatabase());
            }),
            status: ko.computed(function () {
                return appUrl.forStatus(appUrl.currentDatabase());
            }),
            settings: ko.computed(function () {
                return appUrl.forSettings(appUrl.currentDatabase());
            }),
            logs: ko.computed(function () {
                return appUrl.forLogs(appUrl.currentDatabase());
            }),
            alerts: ko.computed(function () {
                return appUrl.forAlerts(appUrl.currentDatabase());
            }),
            indexErrors: ko.computed(function () {
                return appUrl.forIndexErrors(appUrl.currentDatabase());
            }),
            replicationStats: ko.computed(function () {
                return appUrl.forReplicationStats(appUrl.currentDatabase());
            }),
            userInfo: ko.computed(function () {
                return appUrl.forUserInfo(appUrl.currentDatabase());
            })
        };
        return appUrl;
    })();

    
    return appUrl;
});
//# sourceMappingURL=appUrl.js.map

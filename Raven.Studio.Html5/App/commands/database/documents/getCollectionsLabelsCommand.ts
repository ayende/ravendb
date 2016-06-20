import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import queryUtil = require("common/queryUtil");

class getCollectionsLabelsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private collections: collection[], private ownerDb: database) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }


    execute(): JQueryPromise<collection[]> {
        var task = $.Deferred();

        var requests = this.collections.map(collection => {
            return {
                Url: "/queries/",
                Headers: {},
                Query: "?id=" + queryUtil.escapeTerm("Raven/StudioConfig")
            }
        });
        this.post("/multi_get?parallel=yes", JSON.stringify(requests), this.ownerDb, null, 0)
            .done((result) => {
                // if there are no results, then we simply need to leave and go back to normal collection display
                if (!result[0].Result.Results[0])
                    task.resolve(this.collections);
                var arr = result[0].Result.Results[0].Labels;
                for (var i = 0; i < this.collections.length; i++) {
                    if (arr[this.collections[i].collectionName] !== undefined) {
                        this.collections[i].collectionLabel =
                            arr[this.collections[i].collectionName];
                    } else {
                        this.collections[i].collectionLabel = this.collections[i].collectionName;
                    }
                }
                // sort the collections alphabetically
                task.resolve(this.collections.sort((n, r) => {
                    return (n.collectionLabel === r.collectionLabel) ? 0 : (n.collectionLabel > r.collectionLabel) ? 1 : -1;
                }));
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch collection labels", response.responseText, response.statusText);
                // collection labels are only cosmetic, we never want to stop the ui for them.
                task.resolve(response);
            });

        return task;
    }
}

export = getCollectionsLabelsCommand;

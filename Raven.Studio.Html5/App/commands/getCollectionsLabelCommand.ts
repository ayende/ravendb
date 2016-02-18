import commandBase = require("commands/commandBase");
import database = require("models/database");
import collection = require("models/collection");
import getIndexTermsCommand = require("commands/getIndexTermsCommand");

class getCollectionsLabelCommand extends commandBase {

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
                Url: "/indexes/Raven/DocumentsByEntityName",
                Headers: {},
                Query: "?&query=Tag:" + collection.name + "&pageSize=1" + "&resultsTransformer=Raven/LabelsByCollectionName"
            }
        });

        this.post("/multi_get?parallel=yes", JSON.stringify(requests), this.ownerDb, null, 0)
            .done((result) => {
                // if the transformer was not found, we will be sure to create it at this time.
                if (result == null || result[0].Status == 500)
                    this.query("/silverlight/ensureTransformer", null, this.ownerDb)
                        .done(() => task.resolve(this.collections))
                        .fail(() => task.reject("the result transformer: Raven/LabelsByCollectionName was not found"));

                for (var i = 0; i < this.collections.length; i++) {
                    if (result[i].Result.Results != null && result[i].Result.Results.length > 0)
                        if (result[i].Result.Results[0].Label)
                            this.collections[i].label = result[i].Result.Results[0].Label;
                }
                // ensure that the collections are sorted by either entity name or label name.
                task.resolve(this.collections.sort(function(n, r){
                    return (n.getLabel() == r.getLabel()) ? 0 : (n.getLabel() > r.getLabel()) ? 1 : -1;
                }));
            })
            .fail((response: JQueryXHR) => {
                this.reportError("the result transformer: Raven/LabelsByCollectionName was not found", response.responseText, response.statusText);
                task.reject(response);
            });

        return task;
    }
}

export = getCollectionsLabelCommand;

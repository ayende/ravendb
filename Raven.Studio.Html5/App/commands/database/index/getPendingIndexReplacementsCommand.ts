import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");

class getPendingIndexReplacementsCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<indexReplaceDocument>> {
        var resultsSelector = (result: indexReplaceDocumentDto[]) =>
            result.map((dto: indexReplaceDocumentDto) => new indexReplaceDocument(dto));
        var url = "/docs";
        var args = {
            startsWith: indexReplaceDocument.replaceDocumentPrefix,
            start: 0,
            pageSize: 1024
        };

        return this.query(url, args, this.db, resultsSelector);
    }
}

export = getPendingIndexReplacementsCommand;

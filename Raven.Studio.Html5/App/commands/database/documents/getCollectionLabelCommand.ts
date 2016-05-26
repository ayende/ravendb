import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getCollectionLabelCommand extends commandBase {

    constructor(private db: database, private name: string, private pretty: boolean) {
        super();
    }
    ravenEntityLabel: string;
    ravenEntityLabelExists: boolean = false;
    execute(): JQueryPromise<string> {
        var url = "/docs/";
        var args = {
            docId: "Raven/Labels"
        }
        return this.query(url, args, this.db)
            .done((result) => {
                if (result[0] !== undefined) {
                    if (result[0].Labels[this.name] !== undefined) {
                        return this.ravenEntityLabel = result[0].Labels[this.name];
                    }
                }
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to create class code", response.responseText, response.statusText));
    }
    getLabelText(): string {
        return this.ravenEntityLabel !== undefined
            ? this.ravenEntityLabel : this.pretty ? this.name.replace(/__/g, '/') : this.name;
    }
    checkLabel(): boolean {
        return this.ravenEntityLabel !== undefined;
    }
}

export = getCollectionLabelCommand;

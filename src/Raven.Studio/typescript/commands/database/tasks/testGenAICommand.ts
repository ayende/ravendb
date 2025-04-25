import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type TestGenAiScript = Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
type GenAiTestScriptResult = Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult;

class testGenAiCommand extends commandBase {
    constructor(private db: string, private payload: TestGenAiScript) {
        super();
    }  

    execute(): JQueryPromise<GenAiTestScriptResult> {
        const url = endpoints.databases.genAi.adminAiGenaiTest;

        return this.post<GenAiTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to test GenAI`, response.responseText, response.statusText);
            });
    }
}

export = testGenAiCommand; 


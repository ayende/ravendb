import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type TestGenAiScript = Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
type TestEtlScriptResult = Raven.Server.Documents.ETL.Test.TestEtlScriptResult;

class testGenAiCommand extends commandBase {
    constructor(private db: string, private payload: TestGenAiScript) {
        super();
    }  

    execute(): JQueryPromise<TestEtlScriptResult> {
        const url = endpoints.databases.genAi.adminAiGenaiTest;

        return this.post<TestEtlScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to test GenAI`, response.responseText, response.statusText);
            });
    }
}

export = testGenAiCommand; 


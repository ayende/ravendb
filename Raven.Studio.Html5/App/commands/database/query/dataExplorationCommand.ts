import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");
import appUrl = require("common/appUrl");

class dataExplorationCommand extends commandBase {

	xhr: XMLHttpRequest;

    constructor(private request: dataExplorationRequestDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
	    var self = this;
	    var options: JQueryAjaxSettings = {
			xhr: () => {
				self.xhr = new XMLHttpRequest();
				// disable alerts as this request might take a while 
				clearTimeout(commandBase.alertTimeout);
				clearTimeout(commandBase.splashTimerHandle);
				return self.xhr;
			}

		};
		var queryTask = this.query(this.getUrl(), null, this.db, null, options);
        queryTask.fail((response: JQueryXHR) => this.reportError("Error during query", response.responseText, response.statusText));
        return queryTask;
    }

	getUrl() {
		return "/streams/exploration/" + this.urlEncodeArgs(this.request);
    }

    getCsvUrl() {
		var requestWithCsvDownload: any = this.request;
		requestWithCsvDownload.download = "true";
	    requestWithCsvDownload.format = "excel";
		return appUrl.forResourceQuery(this.db) + "/streams/exploration/" + this.urlEncodeArgs(requestWithCsvDownload);
    }
}

export = dataExplorationCommand;
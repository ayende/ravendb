import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import clusterTopology = require("models/database/cluster/clusterTopology");

class getClusterTopologyCommand extends commandBase {

    private serverUrl?: string;

    constructor(serverUrl?: string) {
        super();
        this.serverUrl = serverUrl;
    }

    execute(): JQueryPromise<clusterTopology> {
        
        const args = {
            url: window.location.origin
        };
        const url = endpoints.global.rachisAdmin.clusterTopology;

        return this.query(url, args["url"] ? args : null, null, dto => new clusterTopology(dto));
    }
}

export = getClusterTopologyCommand;

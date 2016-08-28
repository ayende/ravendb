import commandBase = require("commands/commandBase");

class getNewEncryptionKey extends commandBase {

    constructor() {
        super();
    }

    execute() {
        var key = this.query<string>("/studio-tasks/new-encryption-key", null, null);//TODO: use endpoints
        return key;
    }
}

export = getNewEncryptionKey; 

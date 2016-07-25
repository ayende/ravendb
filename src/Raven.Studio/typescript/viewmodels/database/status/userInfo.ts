import getUserInfoCommand = require("commands/database/debug/getUserInfoCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class userInfo extends viewModelBase {

    data = ko.observable<userInfoDto>();
    
    activate(args: any) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchUserInfo());
        this.updateHelpLink('JSVY4P');
        return this.fetchUserInfo();
    }

    fetchUserInfo(): JQueryPromise<userInfoDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getUserInfoCommand(db)
                .execute()
                .done((results: userInfoDto) => this.data(results));
        }

        return null;
    }
}

export = userInfo;

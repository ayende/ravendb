﻿class server {
    name = ko.observable('');
    posCount  = ko.observable(0);
    negCount = ko.observable(0);

    constructor(dto: serverDto) {
        this.name(dto.Name);
        this.posCount(dto.PosCount);
        this.negCount(dto.NegCount);
    }
} 

export = server;
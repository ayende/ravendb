
type menuItem = leafMenuItem | intermediateMenuItem | namedSeparatorMenuItem;

interface leafMenuItem {
    title: string;
    icon?: string;
    route: string;
    moduleId: string;
    hash: KnockoutComputed<string>;
    nav: boolean;
}

interface intermediateMenuItem {
    title: string;
    children: Array<menuItem>;
}

interface namedSeparatorMenuItem {
    title: string;
}
import * as EVENTS from "common/constants/events";
import database = require("models/resources/database");
import router = require("plugins/router");
import {
    generateMenuItems,
    menuItem,
    intermediateMenuItem,
    leafMenuItem } from "common/shell/menuItems";

class menu {

    private $mainMenu: JQuery;
    private $mainMenuAnchors: JQuery;
    private $mainMenuLevels: JQuery;

    private level: number;
    private type: string = 'menu';

    items: Array<menuItem>;

    routerConfiguration(): Array<DurandalRouteConfiguration> {
        return this.items
            .map(getMenuItemDurandalRoutes)
            .reduce((result, next) => result.concat(next), [])
            .reduce((result: any[], next: any) => {
                let nextJson = JSON.stringify(next);
                if (!result.some(x => JSON.stringify(x) === nextJson)) {
                    result.push(next);
                }

                return result;
            }, []) as Array<DurandalRouteConfiguration>;
    }

    static convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
        return {
            route: leaf.route,
            title: leaf.title,
            moduleId: leaf.moduleId,
            nav: leaf.nav,
            dynamicHash: leaf.hash
        };
    }

    constructor(opts: {
        activeDatabase: KnockoutObservable<database>,
        canExposeConfigOverTheWire: KnockoutObservable<boolean>,
        isGlobalAdmin: KnockoutObservable<boolean>
    }) {
        this.items = generateMenuItems(opts);
    }

    initialize () {
        this.$mainMenu = $('#main-menu');
        this.$mainMenuAnchors = $('#main-menu a');
        this.$mainMenuLevels = $('#main-menu [data-level]');

        let self = this;
        this.$mainMenuAnchors.on('click', function (e) {
            let a = this as HTMLAnchorElement;
            let $a = $(a);
            if ($a.is('.back')) {
                return handleBack();
            }

            let deepestOpenLevel = self.getDeepestOpenLevelElement();
            if (deepestOpenLevel && $(deepestOpenLevel).find(a).length === 0) {
                return;
            }

            let $list = $a.closest('.level');
            let hasOpenSubmenus = $list.find('.level-show').length;
            let isOpenable = $a.siblings('.level').length;

            if (!hasOpenSubmenus && isOpenable) {
                $a.parent().children('.level').addClass('level-show');
                e.stopPropagation();
            }

            self.updateLevel();

            function handleBack() {
                $a.closest('.level').removeClass('level-show');
                self.updateLevel();
            }
        });

        this.$mainMenuLevels.on('click', function (e) {
            e.stopPropagation();

            let clickedLevelElement = this as HTMLElement;
            let deepestOpenLevelElement = self.getDeepestOpenLevelElement();
            if (clickedLevelElement === deepestOpenLevelElement) {
                return;
            }

            $(clickedLevelElement)
                .find('.level-show')
                .removeClass('level-show');

            self.updateLevel();


        });

        let $body = $('body');
        $('.menu-collapse-button').click(
            () => $body.toggleClass('menu-collapse'));
    }

    private getDeepestOpenLevelElement() {
        return this.$mainMenuLevels.find('.level-show')
            .toArray()
            .reduce((result: HTMLElement, nextEl: HTMLElement) => {
                if (!result) {
                    return nextEl;
                }

                var resultLevel = this.parseLevel(result);
                var curLevel = this.parseLevel(nextEl);

                if (resultLevel > curLevel) {
                    return result;
                }

                return nextEl;
            }, null);
    }

    private parseLevel(el: HTMLElement) {
        return parseInt(el.dataset['level']);
    }

    private emitLevelChanged() {
        ko.postbox.publish(EVENTS.Menu.LevelChanged, this.level);
    }

    private calculateCurrentLevel() {
        return this.$mainMenu.find('.level-show').length;
    }

    private updateLevel() {
        let newLevel = this.calculateCurrentLevel();
        if (newLevel !== this.level) {
            this.level = newLevel;
            this.emitLevelChanged();
        }

        this.$mainMenu.attr('data-level', this.level);
    }
}

function getMenuItemDurandalRoutes(item: menuItem): Array<DurandalRouteConfiguration> {
    if (item.type === 'intermediate') {
        var intermediateItem = item as intermediateMenuItem;
        return intermediateItem.children
            .map(child => getMenuItemDurandalRoutes(child))
            .reduce((result, next) => result.concat(next), []);
    } else if (item.type === 'leaf') {
        return [ menu.convertToDurandalRoute(item as leafMenuItem) ];
    } 

    return [];
}

export = menu;

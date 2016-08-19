import appUrl = require("common/appUrl");

class menu {

    static appUrls = appUrl.forCurrentDatabase();


    static items: Array<menuItem> = [
        {
            title: "Documents",
            children: [
                {
                    title: "Documents",
                    nav: true,
                    route: "databases/documents",
                    moduleId: "viewmodels/database/documents/documents",
                    hash: menu.appUrls.documents
                } as leafMenuItem,
                {
                    title: "Conflicts",
                    nav: true,
                    route: "database/conflicts",
                    moduleId: "viewmodels/database/conflicts/conflicts",
                    hash: menu.appUrls.conflicts

                } as leafMenuItem,
                {
                    title: "Patch",
                    nav: true,
                    route: "databases/patch(/:recentPatchHash)",
                    moduleId: "viewmodels/database/patch/patch",
                    hash: menu.appUrls.patch
                } as leafMenuItem
            ]
        } as intermediateMenuItem,
        {
            title: "Documents2",
            children: [
                {
                    title: "Documents2",
                    nav: true,
                    route: "databases/documents",
                    moduleId: "viewmodels/database/documents/documents",
                    hash: menu.appUrls.documents
                } as leafMenuItem,
                {
                    title: "Conflicts2",
                    nav: true,
                    route: "database/conflicts",
                    moduleId: "viewmodels/database/conflicts/conflicts",
                    hash: menu.appUrls.conflicts

                } as leafMenuItem,
                {
                    title: "Patch2",
                    nav: true,
                    route: "databases/patch(/:recentPatchHash)",
                    moduleId: "viewmodels/database/patch/patch",
                    hash: menu.appUrls.patch
                } as leafMenuItem
            ]
        } as intermediateMenuItem,
        {
            title: "Documents3",
            children: [
                {
                    title: "Documents3",
                    nav: true,
                    route: "databases/documents",
                    moduleId: "viewmodels/database/documents/documents",
                    hash: menu.appUrls.documents
                } as leafMenuItem,
                {
                    title: "Conflicts3",
                    nav: true,
                    route: "database/conflicts",
                    moduleId: "viewmodels/database/conflicts/conflicts",
                    hash: menu.appUrls.conflicts

                } as leafMenuItem,
                {
                    title: "Patch3",
                    nav: true,
                    route: "databases/patch(/:recentPatchHash)",
                    moduleId: "viewmodels/database/patch/patch",
                    hash: menu.appUrls.patch
                } as leafMenuItem
            ]
        } as intermediateMenuItem
    ];

    static routerConfiguration(): Array<DurandalRouteConfiguration> {
        //TODO: use recurrsion + support separators
        let results = [] as Array<DurandalRouteConfiguration>;
        menu.items.forEach(item0 => {
            if ("children" in item0) {
                var intermediateItem = item0 as intermediateMenuItem;
                intermediateItem.children.forEach(item1 => {
                    results.push(menu.convertToDurandalRoute(item1 as leafMenuItem));
                });
            } else {
                results.push(menu.convertToDurandalRoute(item0 as leafMenuItem));
            }
        });
        return results;
    }

    static convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
        return {
            route: leaf.route,
            title: leaf.title,
            moduleId: leaf.moduleId,
            nav: leaf.nav,
            dynamicHash: leaf.hash
        }
    }

    static setup() {
        // JavaScript Document

        //TODO: replace with TS

        (function (window) {
            var $mainMenu = $('#main-menu');
            var $selectDatabaseContainer = $('.select-database-container');
            var $searchContainer = $('.search-container');

            $selectDatabaseContainer.removeClass('active');
            $searchContainer.removeClass('active');

            function triggerGlobal(evntName: string, ...args: any[]) {
                $(window).trigger(evntName, args);
            }

            (function setupSearch() {
                var $searchInput = $('.search-container input[type=search]');

                $searchInput.click(function (e) {
                    show();
                    e.stopPropagation();
                });

                $('.search-container .autocomplete-list.box-container')
                    .click(e => e.stopPropagation());

                $('.search-container .autocomplete-list.box-container a').on('click', function (e) {
                    e.stopPropagation();
                    hide();
                });

                $(window)
                    .click(hide)
                    .on('menu:levelChanged', hide)
                    .on('resourceSwitcher:show', hide);

                function show() {
                    $searchContainer.addClass('active');
                    triggerGlobal('search:show');
                }

                function hide() {
                    $searchContainer.removeClass('active');
                    triggerGlobal('search:hide');
                }
            } ());

            (function setupResourceSwitcher() {

                var $filter = $('.select-database-container .database-filter');

                $selectDatabaseContainer.click(function (e) {
                    e.stopPropagation();
                    show();
                });

                $('.form-control.btn-toggle.select-database').click(function (e) {
                    if ($(this).is('.active')) {
                        hide();
                    } else {
                        show();
                    }

                    e.stopPropagation();
                });

                $('.select-database-container .box-container a').on('click', function (e) {
                    e.stopPropagation();
                    hide();
                });

                $(window)
                    .click(hide)
                    .on('resourceSwitcher:show', function () {
                        $filter.focus();
                    })
                    .on('menu:levelChanged', hide)
                    .on('search:show', hide);

                function show() {
                    $selectDatabaseContainer.addClass('active');
                    triggerGlobal('resourceSwitcher:show');
                }

                function hide() {
                    $selectDatabaseContainer.removeClass('active');
                    triggerGlobal('resourceSwitcher:hide');
                }
            } ());

            (function setupMainMenu() {

                $('#main-menu a').click(function (e) {
                    var $list = $(this).closest('ul');
                    var hasOpenSubmenus = $list.find('.level-show').length;
                    var isOpenable = $(this).siblings('.level').length;

                    if (!hasOpenSubmenus && isOpenable) {
                        $(this).parent().children('.level').addClass('level-show');
                        emitLevelChanged();
                        e.stopPropagation();
                    }

                    setMenuLevelClass();
                });

                $('#main-menu ul').click(function (e) {
                    $(this).find('.level-show').removeClass('level-show');
                    emitLevelChanged();
                    e.stopPropagation();
                    setMenuLevelClass();
                });

                $('.menu-collapse-button').click(function () {
                    $('body').toggleClass('menu-collapse');
                });

                function emitLevelChanged() {
                    triggerGlobal('menu:levelChanged');
                }

                function setMenuLevelClass() {
                    var level = $mainMenu.find('.level-show');
                    $mainMenu.attr('data-level', level.length);
                }

            } ());

            (function setupToggleButtons() {
                $('.btn-toggle').click(function (e) {
                    var target = $(this).attr('data-target');
                    var targetClass = $(this).attr('data-class');
                    $(target).toggleClass(targetClass);
                });
            } ());

        } (window));

    }
}

export = menu;

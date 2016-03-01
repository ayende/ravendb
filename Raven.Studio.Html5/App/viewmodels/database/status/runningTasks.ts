import viewModelBase = require("viewmodels/viewModelBase");
import killRunningTaskCommand = require("commands/operations/killRunningTaskCommand");
import getRunningTasksCommand = require("commands/operations/getRunningTasksCommand");
import moment = require("moment");
import document = require("models/database/documents/document");
import runningTask = require("models/database/debug/runningTask");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");

type taskType = {
    name: string;
    count: number;
}

class runningTasks extends viewModelBase {

    allTasks = ko.observableArray<runningTask>();
    filterType = ko.observable<string>(null);
    selectedTask = ko.observable<runningTask>();
    noExceptionText = ko.computed(() => !!this.selectedTask() && !!this.selectedTask().exceptionText);

    taskTypes = ko.observableArray<taskType>([]);

    searchText = ko.observable("");
    searchTextThrottled: KnockoutObservable<string>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    sortColumn = ko.observable<string>("logged");
    sortAsc = ko.observable<boolean>(true);
    filteredAndSortedTasks: KnockoutComputed<Array<runningTask>>;
    columnWidths: Array<KnockoutObservable<number>>;

    constructor() {
        super();

        autoRefreshBindingHandler.install();

        this.searchTextThrottled = this.searchText.throttle(200);
        this.activeDatabase.subscribe(() => this.fetchTasks());
        this.updateCurrentNowTime();

        this.filteredAndSortedTasks = ko.computed<Array<runningTask>>(() => {
            var tasks = this.allTasks().filter(t => this.matchesFilterAndSearch(t));
            var column = this.sortColumn();
            var asc = this.sortAsc();

            var sortFunc = (left, right) => {
                if (left[column] === right[column]) { return 0; }
                var test = asc ? ((l, r) => l < r) : ((l, r) => l > r);
                return test(left[column], right[column]) ? 1 : -1;
            }

            return tasks.sort(sortFunc);
        });
    }

    recalculateTaskTypes() {
        var types: taskType[] = [];
        var allTasks = this.allTasks();
        allTasks.forEach(task => {
            var type = task.taskType;
            var existingItem = types.first(t => t.name === type);
            if (existingItem) {
                existingItem.count++;
            } else {
                types.push({ name: type, count: 1 });
            }
        });
        this.taskTypes(types);
    }

    activate(args) {
        super.activate(args);
        this.columnWidths = [
            ko.observable<number>(100),
            ko.observable<number>(300),
            ko.observable<number>(460),
            ko.observable<number>(265),
            ko.observable<number>(100)
        ];
        this.registerColumnResizing();
        this.updateHelpLink('2KH22A');
        return this.fetchTasks();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
        this.unregisterColumnResizing();
    }

    fetchTasks(): JQueryPromise<runningTaskDto[]> {
        var db = this.activeDatabase();
        if (db) {
            var deferred = $.Deferred();
            new getRunningTasksCommand(db)
                .execute()
                .done((results: runningTaskDto[]) => {
                    this.processRunningTasksResults(results);
                    deferred.resolve(results);
                });
            return deferred;
        }

        return null;
    }

    processRunningTasksResults(results: runningTaskDto[]) {
        this.allTasks(results.reverse().map(r => new runningTask(r)));
        this.recalculateTaskTypes();
    }

   
    matchesFilterAndSearch(task: runningTask) {
        var searchTextThrottled = this.searchTextThrottled().toLowerCase();
        var filterType = this.filterType();
        var matchesLogLevel = filterType === null || task.taskType === filterType;
        var matchesSearchText = !searchTextThrottled ||
            (task.description && task.description.toLowerCase().indexOf(searchTextThrottled) >= 0) ||
            (task.exceptionText && task.exceptionText.toLowerCase().indexOf(searchTextThrottled) >= 0);

        return matchesLogLevel && matchesSearchText;
    }

    selectTask(task: runningTask) {
        this.selectedTask(task);
    }

    taskKill(task: runningTask) {
        new killRunningTaskCommand(this.activeDatabase(), task.id).execute()
            .always(() => setTimeout(() => {
                this.selectedTask(null);
                this.fetchTasks();
            }, 1000));
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.selectedTask();
            if (oldSelection) {
                var oldSelectionIndex = this.allTasks.indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allTasks().length - 1) {
                    newSelectionIndex++;
                }

                this.selectedTask(this.allTasks()[newSelectionIndex]);
                var newSelectedRow = $("#runningTasksContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
        }
    }

    ensureRowVisible(row: JQuery) {
        var table = $("#runningTasksTableContainer");
        var scrollTop = table.scrollTop();
        var scrollBottom = scrollTop + table.height();
        var scrollHeight = scrollBottom - scrollTop;

        var rowPosition = row.position();
        var rowTop = rowPosition.top;
        var rowBottom = rowTop + row.height();

        if (rowTop < 0) {
            table.scrollTop(scrollTop + rowTop);
        } else if (rowBottom > scrollHeight) {
            table.scrollTop(scrollTop + (rowBottom - scrollHeight));
        }
    }

    setFilterTypeAll() {
        this.filterType(null);
    }

    setFilterType(value: string) {
        this.filterType(value);
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    sortBy(columnName, logs, event) {
        if (this.sortColumn() === columnName) {
            this.sortAsc(!this.sortAsc());
        }
        else {
            this.sortColumn(columnName);
            this.sortAsc(true);
        }
    }

    registerColumnResizing() {
        var resizingColumn = false;
        var startX = 0;
        var startingWidth = 0;
        var columnIndex = 0;

        $(document).on("mousedown.tasksTableColumnResize", ".column-handle", (e: any) => {
            columnIndex = parseInt($(e.currentTarget).attr("column"));
            startingWidth = this.columnWidths[columnIndex]();
            startX = e.pageX;
            resizingColumn = true;
        });

        $(document).on("mouseup.tasksTableColumnResize", "", (e: any) => {
            resizingColumn = false;
        });

        $(document).on("mousemove.tasksTableColumnResize", "", (e: any) => {
            if (resizingColumn) {
                var targetColumnSize = startingWidth + e.pageX - startX;
                this.columnWidths[columnIndex](targetColumnSize);

                // Stop propagation of the event so the text selection doesn't fire up
                if (e.stopPropagation) e.stopPropagation();
                if (e.preventDefault) e.preventDefault();
                e.cancelBubble = true;
                e.returnValue = false;

                return false;
            }
        });
    }

    unregisterColumnResizing() {
        $(document).off("mousedown.tasksTableColumnResize");
        $(document).off("mouseup.tasksTableColumnResize");
        $(document).off("mousemove.tasksTableColumnResize");
    }
}

export = runningTasks;

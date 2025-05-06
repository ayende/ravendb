import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditGenAiTaskStepId } from "../hooks/useEditGenAiTaskSteps";

interface EditGenAiTaskState {
    taskId: number;
    sourceView: EditAiTaskSourceView;
    currentStep: EditGenAiTaskStepId;
    testStage: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestStage;
    contextTestResults: string[];
    modelOutputTestResults: string[];
    updateScriptTestResult: string;
    globalTestResult: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult;
    isPlaygroundCollapsed: boolean;
    isPlaygroundEditMode: boolean;
    aiConnectionStrings: Record<string, Raven.Client.Documents.Operations.AI.AiConnectionString>;
}

const initialState: EditGenAiTaskState = {
    taskId: null,
    sourceView: "OngoingTasks",
    currentStep: "basic",
    testStage: null,
    contextTestResults: [],
    modelOutputTestResults: [],
    updateScriptTestResult: "",
    globalTestResult: null,
    isPlaygroundCollapsed: false,
    isPlaygroundEditMode: false,
    aiConnectionStrings: {}, // TODO use it to basic step test
};

export const editGenAiTaskSlice = createSlice({
    name: "editGenAiTask",
    initialState,
    reducers: {
        taskIdSet: (state, action: PayloadAction<number>) => {
            state.taskId = action.payload;
        },
        sourceViewSet: (state, action: PayloadAction<EditAiTaskSourceView>) => {
            state.sourceView = action.payload;
        },
        currentStepSet: (state, action: PayloadAction<EditGenAiTaskStepId>) => {
            state.currentStep = action.payload;
        },
        testStageSet: (state, action: PayloadAction<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestStage>) => {
            state.testStage = action.payload;
        },
        contextTestResultsSet: (state, action: PayloadAction<string[]>) => {
            state.contextTestResults = action.payload;
        },
        modelOutputTestResultsSet: (state, action: PayloadAction<string[]>) => {
            state.modelOutputTestResults = action.payload;
        },
        updateScriptTestResultSet: (state, action: PayloadAction<string>) => {
            state.updateScriptTestResult = action.payload;
        },
        globalTestResultSet: (
            state,
            action: PayloadAction<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult>
        ) => {
            state.globalTestResult = action.payload;
        },
        isPlaygroundCollapsedToggled: (state) => {
            state.isPlaygroundCollapsed = !state.isPlaygroundCollapsed;
        },
        isPlaygroundEditModeToggled: (state) => {
            state.isPlaygroundEditMode = !state.isPlaygroundEditMode;
        },
        aiConnectionStringsSet: (
            state,
            action: PayloadAction<Record<string, Raven.Client.Documents.Operations.AI.AiConnectionString>>
        ) => {
            state.aiConnectionStrings = action.payload;
        },
        reset: () => initialState,
    },
});

function selectIsTestOpen(state: RootState): boolean {
    if (state.editGenAiTask.testStage === "CreateContextObjects" && state.editGenAiTask.currentStep === "context") {
        return true;
    }

    if (state.editGenAiTask.testStage === "SendToModel" && state.editGenAiTask.currentStep === "modelInput") {
        return true;
    }

    if (state.editGenAiTask.testStage === "ApplyUpdateScript" && state.editGenAiTask.currentStep === "updateScript") {
        return true;
    }

    return false;
}

export const editGenAiTaskActions = editGenAiTaskSlice.actions;
export const editGenAiTaskSelectors = {
    taskId: (state: RootState) => state.editGenAiTask.taskId,
    isNewTask: (state: RootState) => state.editGenAiTask.taskId == null,
    isEditTask: (state: RootState) => state.editGenAiTask.taskId != null,
    sourceView: (state: RootState) => state.editGenAiTask.sourceView,
    currentStep: (state: RootState) => state.editGenAiTask.currentStep,
    isTestOpen: selectIsTestOpen,
    testStage: (state: RootState) => state.editGenAiTask.testStage,
    contextTestResults: (state: RootState) => state.editGenAiTask.contextTestResults,
    modelOutputTestResults: (state: RootState) => state.editGenAiTask.modelOutputTestResults,
    updateScriptTestResult: (state: RootState) => state.editGenAiTask.updateScriptTestResult,
    isPlaygroundCollapsed: (state: RootState) => state.editGenAiTask.isPlaygroundCollapsed,
    isPlaygroundEditMode: (state: RootState) => state.editGenAiTask.isPlaygroundEditMode,
    globalTestResult: (state: RootState) => state.editGenAiTask.globalTestResult,
    aiConnectionStrings: (state: RootState) => state.editGenAiTask.aiConnectionStrings,
};

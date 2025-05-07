import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditGenAiTaskStepId } from "../hooks/useEditGenAiTaskSteps";
import { services } from "components/hooks/useServices";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";

interface EditGenAiTaskState {
    taskId: number;
    sourceView: EditAiTaskSourceView;
    currentStep: EditGenAiTaskStepId;
    contextTest: loadableData<string[]>;
    modelInputTest: loadableData<string[]>;
    updateScriptTest: loadableData<string>;
    globalTestResult: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult;
    isPlaygroundCollapsed: boolean;
    isPlaygroundEditMode: boolean;
    aiConnectionStrings: Record<string, Raven.Client.Documents.Operations.AI.AiConnectionString>;
    isTestOpen: boolean;
}

const initialState: EditGenAiTaskState = {
    taskId: null,
    sourceView: "OngoingTasks",
    currentStep: "basic",
    contextTest: createIdleState([]),
    modelInputTest: createIdleState([]),
    updateScriptTest: createIdleState(""),
    globalTestResult: null,
    isPlaygroundCollapsed: false,
    isPlaygroundEditMode: false,
    aiConnectionStrings: {}, // TODO use it to basic step test
    isTestOpen: false,
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
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(testContext.pending, (state) => {
                state.contextTest.status = "loading";
            })
            .addCase(testContext.rejected, (state, action) => {
                state.contextTest = createFailureState(action.error.message);
            })
            .addCase(testContext.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.contextTest = createSuccessState(
                    action.payload.Results.map((x) =>
                        x.ContextOutput ? JSON.stringify(x.ContextOutput.Context, null, 4) : null
                    )
                );
            })
            .addCase(testModelInput.pending, (state) => {
                state.modelInputTest.status = "loading";
            })
            .addCase(testModelInput.rejected, (state, action) => {
                state.modelInputTest = createFailureState(action.error.message);
            })
            .addCase(testModelInput.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.modelInputTest = createSuccessState(
                    action.payload.Results.map((x) =>
                        x.ModelOutput ? JSON.stringify(x.ModelOutput.Output, null, 4) : null
                    )
                );
            })
            .addCase(testUpdateScript.pending, (state) => {
                state.updateScriptTest.status = "loading";
            })
            .addCase(testUpdateScript.rejected, (state, action) => {
                state.updateScriptTest = createFailureState(action.error.message);
            })
            .addCase(testUpdateScript.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.updateScriptTest = createSuccessState(
                    action.payload.OutputDocument ? JSON.stringify(action.payload.OutputDocument, null, 4) : null
                );
            });
    },
});

const testContext = createAsyncThunk(
    editGenAiTaskSlice.name + "/testContext",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

const testModelInput = createAsyncThunk(
    editGenAiTaskSlice.name + "/testModelInput",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

const testUpdateScript = createAsyncThunk(
    editGenAiTaskSlice.name + "/testUpdateScript",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

export const editGenAiTaskActions = { ...editGenAiTaskSlice.actions, testContext, testModelInput, testUpdateScript };

export const editGenAiTaskSelectors = {
    taskId: (state: RootState) => state.editGenAiTask.taskId,
    isNewTask: (state: RootState) => state.editGenAiTask.taskId == null,
    isEditTask: (state: RootState) => state.editGenAiTask.taskId != null,
    sourceView: (state: RootState) => state.editGenAiTask.sourceView,
    currentStep: (state: RootState) => state.editGenAiTask.currentStep,
    isTestOpen: (state: RootState) => state.editGenAiTask.isTestOpen,
    contextTest: (state: RootState) => state.editGenAiTask.contextTest,
    modelInputTest: (state: RootState) => state.editGenAiTask.modelInputTest,
    updateScriptTest: (state: RootState) => state.editGenAiTask.updateScriptTest,
    isPlaygroundCollapsed: (state: RootState) => state.editGenAiTask.isPlaygroundCollapsed,
    isPlaygroundEditMode: (state: RootState) => state.editGenAiTask.isPlaygroundEditMode,
    globalTestResult: (state: RootState) => state.editGenAiTask.globalTestResult,
    aiConnectionStrings: (state: RootState) => state.editGenAiTask.aiConnectionStrings,
};

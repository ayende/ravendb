import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditGenAiTaskStepId } from "../hooks/useEditGenAiTaskSteps";

interface EditGenAiTaskState {
    taskId: number;
    sourceView: EditAiTaskSourceView;
    isAdvancedMode: boolean;
    currentStep: EditGenAiTaskStepId;
    isTestOpen: boolean;
}

const initialState: EditGenAiTaskState = {
    taskId: null,
    sourceView: "OngoingTasks",
    isAdvancedMode: false,
    currentStep: "basic",
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
        isAdvancedModeSet: (state, action: PayloadAction<boolean>) => {
            state.isAdvancedMode = action.payload;
        },
        currentStepSet: (state, action: PayloadAction<EditGenAiTaskStepId>) => {
            state.currentStep = action.payload;
        },
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        reset: () => initialState,
    },
});

export const editGenAiTaskActions = editGenAiTaskSlice.actions;
export const editGenAiTaskSelectors = {
    taskId: (state: RootState) => state.editGenAiTask.taskId,
    isNewTask: (state: RootState) => state.editGenAiTask.taskId == null,
    isEditTask: (state: RootState) => state.editGenAiTask.taskId != null,
    sourceView: (state: RootState) => state.editGenAiTask.sourceView,
    isAdvancedMode: (state: RootState) => state.editGenAiTask.isAdvancedMode,
    currentStep: (state: RootState) => state.editGenAiTask.currentStep,
    isTestOpen: (state: RootState) => state.editGenAiTask.isTestOpen,
};

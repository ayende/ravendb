import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import { ReactNode } from "react";
import { EditGenAiTaskStepBasic } from "../partials/steps/EditGenAiTaskStepBasic";
import EditGenAiTaskStepContext from "../partials/steps/EditGenAiTaskStepContext";
import EditGenAiTaskStepModel from "../partials/steps/EditGenAiTaskStepModel";
import EditGenAiTaskStepUpdate from "../partials/steps/EditGenAiTaskStepUpdate";
import EditGenAiTaskStepSummary from "../partials/steps/EditGenAiTaskStepSummary";

export type EditGenAiTaskStepId = "basic" | "context" | "modelInput" | "updateScript" | "summary";

export interface EditGenAiTaskStep {
    id: EditGenAiTaskStepId;
    title: string;
    component: ReactNode;
    isCurrent: boolean;
}

export function useEditGenAiTaskSteps(): EditGenAiTaskStep[] {
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);

    return [
        {
            id: "basic",
            title: "Basic configuration",
            component: <EditGenAiTaskStepBasic />,
            isCurrent: currentStep === "basic",
        },
        {
            id: "context",
            title: "Specify task context",
            component: <EditGenAiTaskStepContext />,
            isCurrent: currentStep === "context",
        },
        {
            id: "modelInput",
            title: "Model input",
            component: <EditGenAiTaskStepModel />,
            isCurrent: currentStep === "modelInput",
        },
        {
            id: "updateScript",
            title: "Update script",
            component: <EditGenAiTaskStepUpdate />,
            isCurrent: currentStep === "updateScript",
        },
        {
            id: "summary",
            title: "Summary",
            component: <EditGenAiTaskStepSummary />,
            isCurrent: currentStep === "summary",
        },
    ];
}

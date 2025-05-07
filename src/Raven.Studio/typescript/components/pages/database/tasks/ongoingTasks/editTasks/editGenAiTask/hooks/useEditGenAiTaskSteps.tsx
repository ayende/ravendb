import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import { ReactNode } from "react";
import { EditGenAiTaskStepBasic, EditGenAiTaskStepBasicFooter } from "../partials/steps/EditGenAiTaskStepBasic";
import { EditGenAiTaskStepContext, EditGenAiTaskStepContextFooter } from "../partials/steps/EditGenAiTaskStepContext";
import { EditGenAiTaskStepModel, EditGenAiTaskStepModelFooter } from "../partials/steps/EditGenAiTaskStepModel";
import { EditGenAiTaskStepUpdate, EditGenAiTaskStepUpdateFooter } from "../partials/steps/EditGenAiTaskStepUpdate";
import { EditGenAiTaskStepSummary, EditGenAiTaskStepSummaryFooter } from "../partials/steps/EditGenAiTaskStepSummary";

export type EditGenAiTaskStepId = "basic" | "context" | "modelInput" | "updateScript" | "summary";

export interface EditGenAiTaskStep {
    id: EditGenAiTaskStepId;
    title: string;
    component: ReactNode;
    footer?: ReactNode;
    isCurrent: boolean;
}

export function useEditGenAiTaskSteps(): EditGenAiTaskStep[] {
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);

    return [
        {
            id: "basic",
            title: "Basic configuration",
            component: <EditGenAiTaskStepBasic />,
            footer: <EditGenAiTaskStepBasicFooter />,
            isCurrent: currentStep === "basic",
        },
        {
            id: "context",
            title: "Specify task context",
            component: <EditGenAiTaskStepContext />,
            footer: <EditGenAiTaskStepContextFooter />,
            isCurrent: currentStep === "context",
        },
        {
            id: "modelInput",
            title: "Model input",
            component: <EditGenAiTaskStepModel />,
            footer: <EditGenAiTaskStepModelFooter />,
            isCurrent: currentStep === "modelInput",
        },
        {
            id: "updateScript",
            title: "Provide document update script",
            component: <EditGenAiTaskStepUpdate />,
            footer: <EditGenAiTaskStepUpdateFooter />,
            isCurrent: currentStep === "updateScript",
        },
        {
            id: "summary",
            title: "Sum up task configuration",
            component: <EditGenAiTaskStepSummary />,
            footer: <EditGenAiTaskStepSummaryFooter />,
            isCurrent: currentStep === "summary",
        },
    ];
}

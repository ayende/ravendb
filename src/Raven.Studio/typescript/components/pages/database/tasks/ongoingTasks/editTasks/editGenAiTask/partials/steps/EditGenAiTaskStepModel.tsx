import { HStack } from "components/common/utilities/HStack";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskModelFields from "../fields/EditGenAiTaskModelFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export function EditGenAiTaskStepModel() {
    return (
        <>
            <AboutViewHeading title="Model input" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskModelFields />
        </>
    );
}

export function EditGenAiTaskStepModelFooter() {
    const dispatch = useAppDispatch();

    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const { handleModelInputTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["prompt", "schemaProvider", "sampleObject", "jsonSchema"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("updateScript"));
        }
    };

    const isTestButtonDisabled = !formValues.playgroundDocument || formValues.playgroundContexts.length === 0;

    return (
        <HStack className="justify-content-between">
            <Button
                variant="secondary"
                className="rounded-pill"
                onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
            >
                <Icon icon="arrow-left" /> Back
            </Button>
            <HStack gap={2}>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !formValues.playgroundDocument,
                            message: "You need to select or provide a document to test this step.",
                        },
                        {
                            isActive: formValues.playgroundContexts.length === 0,
                            message: "Please run test on 'Specify task context' step.",
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleModelInputTest}
                        isSpinning={modelInputTest.status === "loading"}
                        icon="test"
                        disabled={isTestButtonDisabled}
                    >
                        Test model
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </HStack>
        </HStack>
    );
}

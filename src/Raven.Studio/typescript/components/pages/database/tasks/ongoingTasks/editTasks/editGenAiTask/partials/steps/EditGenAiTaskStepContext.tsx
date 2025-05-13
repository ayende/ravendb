import { HStack } from "components/common/utilities/HStack";
import EditGenAiTaskContextFields from "../fields/EditGenAiTaskContextFields";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import EditGenAiTaskPlayground from "../EditGenAiTaskPlayground";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export function EditGenAiTaskStepContext() {
    return (
        <>
            <AboutViewHeading title="Specify task context" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskContextFields />
            <EditGenAiTaskPlayground />
        </>
    );
}

export function EditGenAiTaskStepContextFooter() {
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);

    const { handleContextTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["collectionName", "script"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("modelInput"));
        }
    };

    const isTestButtonDisabled = !formValues.playgroundDocument;

    return (
        <HStack className="justify-content-between">
            <Button
                variant="secondary"
                className="rounded-pill"
                onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))}
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
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleContextTest}
                        isSpinning={contextTest.status === "loading"}
                        disabled={isTestButtonDisabled}
                        icon="test"
                    >
                        Test task context
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </HStack>
        </HStack>
    );
}

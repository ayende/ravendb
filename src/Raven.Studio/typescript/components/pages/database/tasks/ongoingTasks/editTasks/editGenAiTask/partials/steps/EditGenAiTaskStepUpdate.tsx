import { FormGroup, FormLabel } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { HStack } from "components/common/utilities/HStack";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiLoadFile from "../EditGenAiLoadFile";

export function EditGenAiTaskStepUpdate() {
    const { control } = useFormContext<EditGenAiTaskFormData>();

    return (
        <>
            <AboutViewHeading title="Provide document update script" marginBottom={4} icon="ai-etl" />
            <FormGroup>
                <FormLabel className="hstack justify-content-between">
                    Update script
                    <EditGenAiLoadFile name="update" />
                </FormLabel>
                <FormAceEditor control={control} name="update" mode="javascript" />
            </FormGroup>
        </>
    );
}

export function EditGenAiTaskStepUpdateFooter() {
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    const { handleUpdateScriptTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["update"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }
    };

    const isTestButtonDisabled =
        !formValues.playgroundDocument ||
        formValues.playgroundContexts.length === 0 ||
        formValues.playgroundModelOutputs.length === 0;

    return (
        <HStack className="justify-content-between">
            <Button
                variant="secondary"
                className="rounded-pill"
                onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
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
                        {
                            isActive: formValues.playgroundModelOutputs.length === 0,
                            message: "Please run test on 'Model input' step.",
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleUpdateScriptTest}
                        isSpinning={updateScriptTest.status === "loading"}
                        icon="test"
                        disabled={isTestButtonDisabled}
                    >
                        Test update script
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </HStack>
        </HStack>
    );
}

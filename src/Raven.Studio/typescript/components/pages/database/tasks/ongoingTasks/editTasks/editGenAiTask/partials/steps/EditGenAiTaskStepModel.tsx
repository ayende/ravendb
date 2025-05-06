import { HStack } from "components/common/utilities/HStack";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskModelFields from "../fields/EditGenAiTaskModelFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import EditGenAiTaskPlayground from "../EditGenAiTaskPlayground";
import { useAsyncCallback } from "react-async-hook";
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export default function EditGenAiTaskStepModel() {
    const dispatch = useAppDispatch();

    const { control, trigger, setValue, setError, clearErrors } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const taskId = useAppSelector(editGenAiTaskSelectors.taskId);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const globalTestResult = useAppSelector(editGenAiTaskSelectors.globalTestResult);

    const { tasksService } = useServices();

    const handleNext = async () => {
        const isValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("updateScript"));
        }
    };

    const asyncHandleTest = useAsyncCallback(async () => {
        if (!formValues.playgroundDocument) {
            setError("playgroundDocument", { message: "Please provide a document" });
            return;
        } else {
            clearErrors("playgroundDocument");
        }

        const isValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);

        if (!isValid || !formValues.documentId) {
            return;
        }

        const input = structuredClone(globalTestResult.Results);

        for (let i = 0; i < input.length; i++) {
            input[i].ContextOutput.Context = JSON.parse(formValues.playgroundContexts[i].value);
        }

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "SendToModel",
            Input: input,
            Document: JSON.parse(formValues.playgroundDocument),
            DocumentId: formValues.documentId,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await tasksService.testGenAi(databaseName, dto);

        console.log("kalczur result", result);

        dispatch(editGenAiTaskActions.globalTestResultSet(result));

        setValue(
            "playgroundModelOutputs",
            result.Results.map((x) => ({
                value: JSON.stringify(x.ModelOutput?.Output, null, 4),
            }))
        );
        dispatch(
            editGenAiTaskActions.modelOutputTestResultsSet(
                result.Results.map((x) => JSON.stringify(x.ModelOutput?.Output, null, 4))
            )
        );

        dispatch(editGenAiTaskActions.testStageSet("SendToModel"));

        return result;
    });

    return (
        <>
            <AboutViewHeading title="Model input" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskModelFields />
            <HStack className="justify-content-between">
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
                <HStack gap={2}>
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={asyncHandleTest.execute}
                        isSpinning={asyncHandleTest.loading}
                        icon="test"
                    >
                        Test model
                    </ButtonWithSpinner>

                    <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                        Next <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </HStack>
            </HStack>
            <EditGenAiTaskPlayground />
        </>
    );
}

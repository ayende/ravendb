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
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export default function EditGenAiTaskStepContext() {
    const dispatch = useAppDispatch();
    const { control, trigger, setError, clearErrors, setValue } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const taskId = useAppSelector(editGenAiTaskSelectors.taskId);

    const { tasksService } = useServices();

    const handleNext = async () => {
        const isValid = await trigger(["collectionName", "script"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("modelInput"));
        }
    };

    const asyncHandleTest = useAsyncCallback(async () => {
        if (!formValues.playgroundDocument) {
            setError("playgroundDocument", { message: "Please provide a document" });
            return;
        } else {
            clearErrors("playgroundDocument");
        }

        const isValid = await trigger(["collectionName", "script"]);

        if (!isValid || !formValues.documentId) {
            return;
        }

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "CreateContextObjects",
            Input: null,
            Document: formValues.playgroundDocument,
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await tasksService.testGenAi(databaseName, dto);

        dispatch(editGenAiTaskActions.globalTestResultSet(result));

        setValue(
            "playgroundContexts",
            result.Results.map((x) => ({ value: JSON.stringify(x.ContextOutput.Context, null, 4) }))
        );

        dispatch(editGenAiTaskActions.testStageSet("CreateContextObjects"));

        return result;
    });

    return (
        <>
            <AboutViewHeading title="Specify task context" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskContextFields />

            <HStack className="justify-content-between">
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
                <HStack gap={2}>
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={asyncHandleTest.execute}
                        isSpinning={asyncHandleTest.loading}
                    >
                        <Icon icon="test" /> Test task context
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

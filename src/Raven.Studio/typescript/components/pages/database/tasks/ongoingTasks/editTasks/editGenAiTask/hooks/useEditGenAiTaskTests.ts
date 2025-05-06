import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskSelectors, editGenAiTaskActions } from "../store/editGenAiTaskSlice";
import { editGenAiTaskUtils } from "../utils/editGenAiTaskUtils";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";

export function useEditGenAiTaskTests() {
    const dispatch = useAppDispatch();
    const { control, trigger, setError, clearErrors, setValue } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const taskId = useAppSelector(editGenAiTaskSelectors.taskId);
    const globalTestResult = useAppSelector(editGenAiTaskSelectors.globalTestResult);

    const handleDocumentTrigger = async (): Promise<boolean> => {
        if (!formValues.playgroundDocument) {
            setError("playgroundDocument", { message: "Please provide a document" });
            return false;
        } else {
            clearErrors("playgroundDocument");
            return true;
        }
    };

    const handleContextTest = async () => {
        const isDocumentValid = await handleDocumentTrigger();
        if (!isDocumentValid) {
            return;
        }

        const areTestRelatedFieldsValid = await trigger(["collectionName", "script"]);
        if (!areTestRelatedFieldsValid) {
            return;
        }

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            TestStage: "CreateContextObjects",
            Input: null,
            Document: JSON.parse(formValues.playgroundDocument),
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await dispatch(editGenAiTaskActions.testContext({ databaseName, dto })).unwrap();

        setValue(
            "playgroundContexts",
            result.Results.map((x) => ({
                value: JSON.stringify(x.ContextOutput.Context, null, 4),
            }))
        );
    };

    const handleModelInputTest = async () => {
        const isDocumentValid = await handleDocumentTrigger();
        if (!isDocumentValid) {
            return;
        }

        const areTestRelatedFieldsValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);
        if (!areTestRelatedFieldsValid) {
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
            DocumentId: undefined,
            IsDelete: false,
            Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
        };

        const result = await dispatch(editGenAiTaskActions.testModelInput({ databaseName, dto })).unwrap();

        setValue(
            "playgroundModelOutputs",
            result.Results.map((x) => ({
                value: JSON.stringify(x.ModelOutput?.Output, null, 4),
            }))
        );
    };

    return {
        handleContextTest,
        handleModelInputTest,
    };
}

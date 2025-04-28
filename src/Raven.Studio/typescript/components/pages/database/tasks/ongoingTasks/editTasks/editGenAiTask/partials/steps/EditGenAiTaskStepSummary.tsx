import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { HStack } from "components/common/utilities/HStack";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppDispatch } from "components/store";
import { editGenAiTaskActions } from "../../store/editGenAiTaskSlice";

export default function EditGenAiTaskStepSummary() {
    const dispatch = useAppDispatch();
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({ control });

    return (
        <>
            <pre>{JSON.stringify(formValues, null, 2)}</pre>

            <HStack gap={2} className="justify-content-between">
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>

                <Button type="submit" variant="primary" className="rounded-pill">
                    Save <Icon icon="save" margin="m-0" />
                </Button>
            </HStack>
        </>
    );
}

import { useAppDispatch } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { HStack } from "components/common/utilities/HStack";
import EditGenAiTaskBasicFields from "../fields/EditGenAiTaskBasicFields";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";

export function EditGenAiTaskStepBasic() {
    const dispatch = useAppDispatch();

    const { trigger } = useFormContext<EditGenAiTaskFormData>();

    const handleNext = async () => {
        const isValid = await trigger(["name", "state", "responsibleNode", "connectionStringName"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("context"));
        }
    };

    return (
        <div>
            <EditGenAiTaskBasicFields />

            <HStack gap={2} className="justify-content-end">
                <Button variant="info" className="rounded-pill">
                    <Icon icon="test" /> Test connection
                </Button>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </HStack>
        </div>
    );
}

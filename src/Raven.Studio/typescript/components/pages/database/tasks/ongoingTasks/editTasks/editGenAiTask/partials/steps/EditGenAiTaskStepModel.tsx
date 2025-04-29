import { HStack } from "components/common/utilities/HStack";
import { useAppDispatch } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskModelFields from "../fields/EditGenAiTaskModelFields";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";

export default function EditGenAiTaskStepModel() {
    const dispatch = useAppDispatch();
    const { trigger } = useFormContext<EditGenAiTaskFormData>();

    const handleNext = async () => {
        const isValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("updateScript"));
        }
    };

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
                    <Button variant="info" className="rounded-pill">
                        <Icon icon="test" /> Test model
                    </Button>

                    <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                        Next <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </HStack>
            </HStack>
        </>
    );
}

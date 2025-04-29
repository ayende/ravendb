import { HStack } from "components/common/utilities/HStack";
import EditGenAiTaskContextFields from "../fields/EditGenAiTaskContextFields";
import { useAppDispatch } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";

export default function EditGenAiTaskStepContext() {
    const dispatch = useAppDispatch();
    const { trigger } = useFormContext<EditGenAiTaskFormData>();

    const handleNext = async () => {
        const isValid = await trigger(["collectionName", "documentId", "script"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("modelInput"));
        }
    };

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
                    <Button variant="info" className="rounded-pill">
                        <Icon icon="test" /> Test task context
                    </Button>

                    <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                        Next <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </HStack>
            </HStack>
        </>
    );
}

import { FormGroup, FormLabel } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useAppDispatch } from "components/store";
import { useFormContext } from "react-hook-form";
import { editGenAiTaskActions } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { HStack } from "components/common/utilities/HStack";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import EditGenAiTaskPlayground from "../EditGenAiTaskPlayground";

export default function EditGenAiTaskStepUpdate() {
    const dispatch = useAppDispatch();
    const { control } = useFormContext();
    const { trigger } = useFormContext<EditGenAiTaskFormData>();

    const handleNext = async () => {
        const isValid = await trigger(["update"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }
    };

    return (
        <>
            <AboutViewHeading title="Provide document update script" marginBottom={4} icon="ai-etl" />
            <FormGroup>
                <FormLabel>Update script</FormLabel>
                <FormAceEditor control={control} name="update" mode="javascript" />
            </FormGroup>

            <HStack className="justify-content-between">
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
                <HStack gap={2}>
                    <Button variant="info" className="rounded-pill">
                        <Icon icon="test" /> Test update script
                    </Button>

                    <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                        Next <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </HStack>
            </HStack>
            <EditGenAiTaskPlayground />
        </>
    );
}

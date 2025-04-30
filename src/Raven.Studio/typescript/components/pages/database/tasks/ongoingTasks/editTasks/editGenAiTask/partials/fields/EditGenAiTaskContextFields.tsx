import { FormLabel, FormSelectCreatable } from "components/common/Form";
import { FormGroup } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";

export default function EditGenAiTaskContextFields() {
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    return (
        <>
            <FormGroup>
                <FormLabel>Collection Name</FormLabel>
                <FormSelectCreatable control={control} name="collectionName" options={collectionOptions} />
            </FormGroup>
            <FormGroup>
                <FormLabel>Script</FormLabel>
                <FormAceEditor control={control} name="script" mode="javascript" />
            </FormGroup>
        </>
    );
}

import { FieldPath, useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import fileImporter from "common/fileImporter";

interface EditGenAiLoadFileProps {
    name: FieldPath<EditGenAiTaskFormData>;
}

export default function EditGenAiLoadFile({ name }: EditGenAiLoadFileProps) {
    const { setValue } = useFormContext<EditGenAiTaskFormData>();

    const id = name + "file";

    return (
        <div>
            <FormLabel className="btn btn-link btn-xs text-right m-0" htmlFor={id}>
                <Icon icon="upload" />
                Load from a file
            </FormLabel>
            <input
                id={id}
                type="file"
                className="d-none"
                onChange={(e) =>
                    fileImporter.readAsText(e.currentTarget, (x) => setValue(name, x, { shouldValidate: true }))
                }
            />
        </div>
    );
}

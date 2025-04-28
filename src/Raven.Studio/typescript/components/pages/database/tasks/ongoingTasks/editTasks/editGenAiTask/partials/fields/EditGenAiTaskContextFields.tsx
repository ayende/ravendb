import { FormSelectAutocomplete } from "components/common/Form";

import Code from "components/common/Code";
import { FormLabel, FormSelectCreatable } from "components/common/Form";
import { FormGroup } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import documentMetadata from "models/database/documents/documentMetadata";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { SelectOption } from "components/common/select/Select";

export default function EditGenAiTaskContextFields() {
    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const asyncGetDocumentIdOptions = useAsyncDebounce(
        async () => {
            const result = await databasesService.getDocumentsMetadataByIDPrefix(
                formValues.documentId,
                10,
                databaseName
            );
            return result.map((x) => x["@metadata"]["@id"]).map((x) => ({ value: x, label: x }));
        },
        [formValues.documentId],
        300
    );

    const asyncGetDocument = useAsyncDebounce(
        async () => {
            const result = await databasesService.getDocumentWithMetadata(formValues.documentId, databaseName);
            const docDto = result.toDto(true);
            const metaDto = docDto["@metadata"];
            documentMetadata.filterMetadata(metaDto);
            return docDto;
        },
        [formValues.documentId],
        300
    );

    return (
        <>
            <FormGroup>
                <FormLabel>Collection Name</FormLabel>
                <FormSelectCreatable control={control} name="collectionName" options={collectionOptions} />
            </FormGroup>
            <FormGroup>
                <FormLabel>Document ID</FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name="documentId"
                    options={asyncGetDocumentIdOptions.result ?? []}
                    isLoading={asyncGetDocumentIdOptions.loading}
                />
            </FormGroup>
            {asyncGetDocument.result && (
                <FormGroup>
                    <FormLabel>Document</FormLabel>
                    <Code code={JSON.stringify(asyncGetDocument.result, null, 2)} language="json" />
                </FormGroup>
            )}
            <FormGroup>
                <FormLabel>Script</FormLabel>
                <FormAceEditor control={control} name="script" mode="javascript" />
            </FormGroup>
        </>
    );
}

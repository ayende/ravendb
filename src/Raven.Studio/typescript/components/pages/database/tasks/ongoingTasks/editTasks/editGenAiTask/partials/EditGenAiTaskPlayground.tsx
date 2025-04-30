import { useFieldArray, useWatch } from "react-hook-form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { HStack } from "components/common/utilities/HStack";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { VStack } from "components/common/utilities/VStack";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import documentMetadata from "models/database/documents/documentMetadata";
import {
    FormAceEditor,
    FormGroup,
    FormLabel,
    FormSelectAutocomplete,
    FormValidationMessage,
} from "components/common/Form";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";

export default function EditGenAiTaskPlayground() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);

    const {
        control,
        setValue,
        formState: { errors },
    } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({ control });

    const contextsFieldsArray = useFieldArray({
        control,
        name: "playgroundContexts",
    });

    const modelOutputsFieldsArray = useFieldArray({
        control,
        name: "playgroundModelOutputs",
    });

    const { databasesService } = useServices();

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

    useAsyncDebounce(
        async () => {
            const result = await databasesService.getDocumentWithMetadata(formValues.documentId, databaseName);
            const docDto = result.toDto(true);
            const metaDto = docDto["@metadata"];
            documentMetadata.filterMetadata(metaDto);

            setValue("playgroundDocument", JSON.stringify(docDto, null, 4));
        },
        [formValues.documentId],
        300
    );

    // TODO dashed border styles
    // TODO info tooltip

    return (
        <div className="mt-4">
            <HStack>
                <div>
                    Playground
                    <Icon icon="info" margin="ms-1" />
                </div>
                <div style={{ border: "1px dashed #555" }} className="flex-grow mx-2"></div>
                <div>
                    <Button variant="link" size="sm">
                        <Icon icon="collapse-vertical" />
                        Collapse
                    </Button>
                </div>
            </HStack>
            <div className="panel-bg-1 border border-secondary rounded-2">
                <Tab.Container id="playground-tabs" defaultActiveKey="document">
                    <Nav variant="pills" className="panel-bg-2 border-bottom border-secondary p-2">
                        <Nav.Item className="all-reset">
                            <Nav.Link eventKey="document" color="link">
                                <Icon icon="document" />
                                Document
                            </Nav.Link>
                        </Nav.Item>
                        {currentStep === "modelInput" && contextsFieldsArray.fields.length > 0 && (
                            <Nav.Item>
                                <Nav.Link eventKey="context">
                                    <Icon icon="indent" />
                                    Context
                                </Nav.Link>
                            </Nav.Item>
                        )}
                        {currentStep === "updateScript" && modelOutputsFieldsArray.fields.length > 0 && (
                            <Nav.Item>
                                <Nav.Link eventKey="modelOutput">
                                    <Icon icon="resources" />
                                    Model output
                                </Nav.Link>
                            </Nav.Item>
                        )}
                    </Nav>

                    <Tab.Content className="p-3">
                        <Tab.Pane eventKey="document">
                            {!formValues.playgroundDocument && (
                                <VStack className="align-items-center">
                                    {errors.playgroundDocument && (
                                        <FormValidationMessage className="d-flex justify-content-center mt-2">
                                            {errors.playgroundDocument.message}
                                        </FormValidationMessage>
                                    )}
                                    <FormGroup>
                                        <FormLabel>Select document ID from the collection</FormLabel>
                                        <FormSelectAutocomplete
                                            control={control}
                                            name="documentId"
                                            options={asyncGetDocumentIdOptions.result ?? []}
                                            isLoading={asyncGetDocumentIdOptions.loading}
                                        />
                                    </FormGroup>
                                    <div className="mb-2">or</div>
                                    <Button variant="primary" onClick={() => setValue("playgroundDocument", "{}")}>
                                        <Icon icon="edit" />
                                        Provide manually
                                    </Button>
                                </VStack>
                            )}
                            {formValues.playgroundDocument && (
                                <div>
                                    <HStack className="justify-content-end mb-1">
                                        <Button
                                            variant="link"
                                            size="sm"
                                            onClick={() => setValue("playgroundDocument", "")}
                                        >
                                            <Icon icon="reset" />
                                            Reset selection
                                        </Button>
                                    </HStack>
                                    <FormAceEditor control={control} name="playgroundDocument" mode="json" />
                                </div>
                            )}
                        </Tab.Pane>
                        <Tab.Pane eventKey="context">
                            {contextsFieldsArray.fields.map((field, idx) => (
                                <FormAceEditor
                                    key={field.id}
                                    control={control}
                                    name={`playgroundContexts.${idx}`}
                                    mode="json"
                                />
                            ))}
                        </Tab.Pane>
                        <Tab.Pane eventKey="modelOutput">
                            {modelOutputsFieldsArray.fields.map((field, idx) => (
                                <FormAceEditor
                                    key={field.id}
                                    control={control}
                                    name={`playgroundModelOutputs.${idx}`}
                                    mode="json"
                                />
                            ))}
                        </Tab.Pane>
                    </Tab.Content>
                </Tab.Container>
            </div>
        </div>
    );
}

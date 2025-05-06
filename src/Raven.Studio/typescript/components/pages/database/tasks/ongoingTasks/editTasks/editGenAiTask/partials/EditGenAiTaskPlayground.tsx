import { useFieldArray, useWatch } from "react-hook-form";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { HStack } from "components/common/utilities/HStack";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { VStack } from "components/common/utilities/VStack";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
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
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import Collapse from "react-bootstrap/Collapse";
import useConfirm from "components/common/ConfirmDialog";
import RichAlert from "components/common/RichAlert";
import classNames from "classnames";

export default function EditGenAiTaskPlayground() {
    const dispatch = useAppDispatch();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const isPlaygroundCollapsed = useAppSelector(editGenAiTaskSelectors.isPlaygroundCollapsed);
    const isPlaygroundEditMode = useAppSelector(editGenAiTaskSelectors.isPlaygroundEditMode);
    const contextTestResults = useAppSelector(editGenAiTaskSelectors.contextTestResults);
    const modelOutputTestResults = useAppSelector(editGenAiTaskSelectors.modelOutputTestResults);
    const updateScriptTestResult = useAppSelector(editGenAiTaskSelectors.updateScriptTestResult);

    const {
        control,
        setValue,
        formState: { errors },
    } = useFormContext<EditGenAiTaskFormData>();

    const confirm = useConfirm();

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

    const handleEditModeToggle = async () => {
        const isConfirmed = await confirm({
            title: "You’re about to enter Playground edit mode",
            message:
                "While you’ll be able to manipulate the element, please be aware that any changes made won’t be saved to the actual document.",
            actionColor: "warning",
            confirmIcon: "arrow-right",
            confirmText: "Enter edit mode",
            icon: "edit",
        });

        if (isConfirmed) {
            dispatch(editGenAiTaskActions.isPlaygroundEditModeToggled());
        }
    };

    // TODO dashed border styles
    // TODO info tooltip
    // TODO click edit

    const getActiveTab = () => {
        if (currentStep === "modelInput" && contextTestResults.length > 0) {
            return "context";
        }

        if (currentStep === "updateScript" && modelOutputTestResults.length > 0) {
            return "modelOutput";
        }

        return "document";
    };

    return (
        <div className="mt-4">
            <HStack>
                <div>
                    Playground
                    <Icon icon="info" margin="ms-1" />
                </div>
                <div style={{ border: "1px dashed #555" }} className="flex-grow mx-2"></div>
                <div>
                    <Button
                        variant="link"
                        size="xs"
                        onClick={() => dispatch(editGenAiTaskActions.isPlaygroundCollapsedToggled())}
                    >
                        <Icon icon={isPlaygroundCollapsed ? "expand-vertical" : "collapse-vertical"} />
                        {isPlaygroundCollapsed ? "Expand" : "Collapse"}
                    </Button>
                </div>
            </HStack>
            <Collapse in={!isPlaygroundCollapsed}>
                <div className="panel-bg-1 border border-secondary rounded-2 mt-1">
                    <Tab.Container id="playground-tabs" defaultActiveKey={getActiveTab()}>
                        <HStack className="panel-bg-2 border-bottom border-secondary p-2 justify-content-between">
                            <Nav>
                                <Nav.Item>
                                    <Nav.Link
                                        eventKey="document"
                                        className={classNames({ "text-danger": !!errors.playgroundDocument })}
                                    >
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
                            {!isPlaygroundEditMode && (
                                <Button
                                    variant="secondary"
                                    className="rounded-pill py-1"
                                    size="sm"
                                    onClick={handleEditModeToggle}
                                >
                                    <Icon icon="edit" />
                                    Edit mode
                                </Button>
                            )}
                        </HStack>

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
                                <RichAlert variant="info" className="mb-3">
                                    This tab provides a comprehensive overview of the context being transmitted to the
                                    AI model. Here, you can explore the various elements and data points that contribute
                                    to the model&apos;s understanding and processing capabilities.
                                </RichAlert>
                                <VStack gap={2}>
                                    {contextsFieldsArray.fields.map((field, idx) => (
                                        <FormAceEditor
                                            key={field.id}
                                            control={control}
                                            name={`playgroundContexts.${idx}.value`}
                                            mode="json"
                                        />
                                    ))}
                                </VStack>
                            </Tab.Pane>
                            <Tab.Pane eventKey="modelOutput">
                                <RichAlert variant="info" className="mb-3">
                                    Within this section, you can discover the results generated by AI model.Within this
                                    section, you can discover the results generated by AI model.
                                </RichAlert>
                                <VStack gap={2}>
                                    {modelOutputsFieldsArray.fields.map((field, idx) => (
                                        <FormAceEditor
                                            key={field.id}
                                            control={control}
                                            name={`playgroundModelOutputs.${idx}.value`}
                                            mode="json"
                                        />
                                    ))}
                                </VStack>
                            </Tab.Pane>
                        </Tab.Content>
                    </Tab.Container>
                </div>
            </Collapse>
        </div>
    );
}

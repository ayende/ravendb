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
import { useEffect } from "react";
import { Switch } from "components/common/Checkbox";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiTaskFormVirtualList from "./EditGenAiTaskFormVirtualList";

export default function EditGenAiTaskPlayground() {
    const dispatch = useAppDispatch();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const isPlaygroundCollapsed = useAppSelector(editGenAiTaskSelectors.isPlaygroundCollapsed);
    const isPlaygroundEditMode = useAppSelector(editGenAiTaskSelectors.isPlaygroundEditMode);
    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);
    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);
    const isDocumentInfoVisible = useAppSelector(editGenAiTaskSelectors.isDocumentInfoVisible);
    const isContextInfoVisible = useAppSelector(editGenAiTaskSelectors.isContextInfoVisible);
    const isModelInputInfoVisible = useAppSelector(editGenAiTaskSelectors.isModelInputInfoVisible);

    const {
        control,
        setValue,
        formState: { errors },
        clearErrors,
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

    const handleEditModeToggle = async (isSelected: boolean): Promise<boolean> => {
        if (isSelected) {
            const isConfirmed = await confirm({
                title: "You’re about to enter Playground edit mode",
                message:
                    "While you’ll be able to manipulate the element, please be aware that any changes made won’t be saved to the actual document.",
                actionColor: "warning",
                confirmIcon: "arrow-right",
                confirmText: "Enter edit mode",
                icon: "edited",
                size: "lg",
            });

            if (isConfirmed) {
                dispatch(editGenAiTaskActions.isPlaygroundEditModeToggled());
                return true;
            } else {
                return false;
            }
        } else {
            dispatch(editGenAiTaskActions.isPlaygroundEditModeToggled());
            return true;
        }
    };

    const handleProvideContentManually = async () => {
        if (await handleEditModeToggle(!isPlaygroundEditMode)) {
            setValue("playgroundDocument", "{}", { shouldValidate: true });
        }
    };

    // TODO info tooltip

    const getActiveTab = () => {
        if (currentStep === "modelInput" && contextTest.data?.length > 0) {
            return "context";
        }

        if (currentStep === "updateScript" && modelInputTest.data?.length > 0) {
            return "modelOutput";
        }

        return "document";
    };

    useEffect(() => {
        if (formValues.playgroundDocument) {
            clearErrors("playgroundDocument");
        }
    }, [formValues.playgroundDocument]);

    return (
        <div className="playground">
            <HStack>
                <div>
                    Playground
                    <Icon icon="info" margin="ms-1" />
                </div>
                <div className="playground-line"></div>
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
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: currentStep !== "context",
                                            message:
                                                "This configuration doesn’t give any additional context to the active step.",
                                        }}
                                    >
                                        <Nav.Link
                                            eventKey="document"
                                            className={classNames(
                                                { "text-danger": !!errors.playgroundDocument },
                                                { "text-muted": currentStep !== "context" }
                                            )}
                                        >
                                            <Icon icon="document" />
                                            Document
                                        </Nav.Link>
                                    </ConditionalPopover>
                                </Nav.Item>
                                {(currentStep === "modelInput" || currentStep === "updateScript") &&
                                    contextsFieldsArray.fields.length > 0 && (
                                        <Nav.Item>
                                            <ConditionalPopover
                                                conditions={{
                                                    isActive: currentStep !== "modelInput",
                                                    message:
                                                        "This configuration doesn’t give any additional context to the active step.",
                                                }}
                                            >
                                                <Nav.Link
                                                    eventKey="context"
                                                    className={classNames({
                                                        "text-muted": currentStep !== "modelInput",
                                                    })}
                                                >
                                                    <Icon icon="indent" />
                                                    Context
                                                </Nav.Link>
                                            </ConditionalPopover>
                                        </Nav.Item>
                                    )}
                                {currentStep === "updateScript" && modelOutputsFieldsArray.fields.length > 0 && (
                                    <Nav.Item>
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: currentStep !== "updateScript",
                                                message:
                                                    "This configuration doesn’t give any additional context to the active step.",
                                            }}
                                        >
                                            <Nav.Link
                                                eventKey="modelOutput"
                                                className={classNames({ "text-muted": currentStep !== "updateScript" })}
                                            >
                                                <Icon icon="resources" />
                                                Model output
                                            </Nav.Link>
                                        </ConditionalPopover>
                                    </Nav.Item>
                                )}
                            </Nav>
                            <Switch
                                id="editMode"
                                toggleSelection={(e) => handleEditModeToggle(e.target.checked)}
                                selected={isPlaygroundEditMode}
                                color="info"
                                className="mt-1"
                            >
                                Edit mode
                            </Switch>
                        </HStack>

                        <Tab.Content className="p-3">
                            <Tab.Pane eventKey="document">
                                {!formValues.playgroundDocument && (
                                    <VStack className="align-items-center py-3">
                                        {isDocumentInfoVisible && (
                                            <RichAlert
                                                variant="info"
                                                className="mb-3"
                                                onCancel={() =>
                                                    dispatch(editGenAiTaskActions.isDocumentInfoVisibleSet(false))
                                                }
                                            >
                                                In this section, you have the option to choose a document for testing.
                                                You can either pick from the existing documents or manually enter a new
                                                one.
                                            </RichAlert>
                                        )}
                                        <EditModeWarning />
                                        {errors.playgroundDocument && (
                                            <FormValidationMessage className="d-flex justify-content-center mt-2">
                                                {errors.playgroundDocument.message}
                                            </FormValidationMessage>
                                        )}
                                        <FormGroup>
                                            <FormLabel>Choose document from the selected collection</FormLabel>
                                            <FormSelectAutocomplete
                                                control={control}
                                                name="documentId"
                                                placeholder="E.g. Posts/01"
                                                options={asyncGetDocumentIdOptions.result ?? []}
                                                isLoading={asyncGetDocumentIdOptions.loading}
                                                onMenuClose={() => clearErrors("playgroundDocument")}
                                            />
                                        </FormGroup>
                                        <Button variant="link" onClick={handleProvideContentManually} size="sm">
                                            <Icon icon="edit" />I want to provide content manually
                                        </Button>
                                    </VStack>
                                )}
                                {formValues.playgroundDocument && (
                                    <div>
                                        <EditModeWarning />
                                        <HStack
                                            className={classNames("mb-1", {
                                                "justify-content-end": !formValues.documentId,
                                                "justify-content-between": formValues.documentId,
                                            })}
                                        >
                                            {formValues.documentId && (
                                                <div>
                                                    Selected document: <strong>{formValues.documentId}</strong>
                                                </div>
                                            )}
                                            <Button
                                                variant="link"
                                                size="sm"
                                                onClick={() => setValue("playgroundDocument", "")}
                                            >
                                                <Icon icon="reset" />
                                                Reset selection
                                            </Button>
                                        </HStack>
                                        <FormAceEditor
                                            control={control}
                                            name="playgroundDocument"
                                            mode="json"
                                            readOnly={!isPlaygroundEditMode}
                                        />
                                    </div>
                                )}
                            </Tab.Pane>
                            <Tab.Pane eventKey="context">
                                {isContextInfoVisible && (
                                    <RichAlert
                                        variant="info"
                                        className="mb-3"
                                        onCancel={() => dispatch(editGenAiTaskActions.isContextInfoVisibleSet(false))}
                                    >
                                        This tab provides a comprehensive overview of the context being transmitted to
                                        the AI model. Here, you can explore the various elements and data points that
                                        contribute to the model&apos;s understanding and processing capabilities.
                                    </RichAlert>
                                )}
                                <EditModeWarning />
                                <div
                                    style={{ height: getVirtualListHeight(contextsFieldsArray.fields.length) }}
                                    className="d-flex"
                                >
                                    <EditGenAiTaskFormVirtualList
                                        fields={contextsFieldsArray.fields}
                                        name="playgroundContexts"
                                        isReadOnly={!isPlaygroundEditMode}
                                    />
                                </div>
                            </Tab.Pane>
                            <Tab.Pane eventKey="modelOutput">
                                {isModelInputInfoVisible && (
                                    <RichAlert
                                        variant="info"
                                        className="mb-3"
                                        onCancel={() =>
                                            dispatch(editGenAiTaskActions.isModelInputInfoVisibleSet(false))
                                        }
                                    >
                                        Within this section, you can discover the results generated by AI model.Within
                                        this section, you can discover the results generated by AI model.
                                    </RichAlert>
                                )}
                                <EditModeWarning />
                                <div
                                    style={{ height: getVirtualListHeight(modelOutputsFieldsArray.fields.length) }}
                                    className="d-flex"
                                >
                                    <EditGenAiTaskFormVirtualList
                                        fields={modelOutputsFieldsArray.fields}
                                        name="playgroundModelOutputs"
                                        isReadOnly={!isPlaygroundEditMode}
                                    />
                                </div>
                            </Tab.Pane>
                        </Tab.Content>
                    </Tab.Container>
                </div>
            </Collapse>
        </div>
    );
}

function getVirtualListHeight(count: number): `${number}px` {
    if (count <= 1) {
        return "200px";
    }

    if (count === 2) {
        return "450px";
    }

    return "500px";
}

function EditModeWarning() {
    const dispatch = useAppDispatch();
    const isEditModeWarningVisible = useAppSelector(editGenAiTaskSelectors.isEditModeWarningVisible);
    const isPlaygroundEditMode = useAppSelector(editGenAiTaskSelectors.isPlaygroundEditMode);

    if (!isEditModeWarningVisible || !isPlaygroundEditMode) {
        return null;
    }

    return (
        <RichAlert
            variant="warning"
            className="mb-2"
            onCancel={() => dispatch(editGenAiTaskActions.isEditModeWarningVisibleSet(false))}
        >
            You&apos;re currently in Edit mode. You&apos;ll be able to manipulate the content, but beware that any
            changes made won&apos;t be saved outside the Playground.
        </RichAlert>
    );
}

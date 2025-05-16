import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { HStack } from "components/common/utilities/HStack";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { AboutViewHeading } from "components/common/AboutView";
import useBoolean from "components/hooks/useBoolean";
import AceEditor from "components/common/AceEditor";
import { AceEditorMode } from "components/models/aceEditor";
import Collapse from "react-bootstrap/Collapse";
import { ThemeColor } from "components/models/common";

export function EditGenAiTaskStepSummary() {
    const dispatch = useAppDispatch();

    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);

    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    return (
        <>
            <AboutViewHeading title="Sum up task configuration" marginBottom={4} icon="ai-etl" />
            <HStack className="justify-content-between">
                <div>Basic configuration</div>
                <Button variant="link" onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))} size="sm">
                    <Icon icon="edit" /> Edit
                </Button>
            </HStack>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <HStack className="justify-content-between">
                    <div>Task name</div>
                    <div>{formValues.name}</div>
                </HStack>
                <HStack className="justify-content-between">
                    <div>Task state</div>
                    <div className={getBooleanColorClass(formValues.state === "Enabled")}>{formValues.state}</div>
                </HStack>
                {formValues.responsibleNode && (
                    <HStack className="justify-content-between">
                        <div>Responsible node</div>
                        <div>
                            <Icon icon="node" color="node" />
                            {formValues.responsibleNode}
                        </div>
                    </HStack>
                )}
                <HStack className="justify-content-between">
                    <div>Connection string</div>
                    <div>{formValues.connectionStringName}</div>
                </HStack>
                {isEditTask && (
                    <HStack className="justify-content-between">
                        <div>Regenerate all documents</div>
                        <div className={getBooleanColorClass(formValues.isResetScript)}>
                            {formValues.isResetScript ? "Enabled" : "Disabled"}
                        </div>
                    </HStack>
                )}
            </div>
            <HStack className="justify-content-between mt-4">
                <div>Specify task context</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </HStack>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <HStack className="justify-content-between">
                    <div>Collection name</div>
                    <div>{formValues.collectionName}</div>
                </HStack>
                <RowWithPreview label="Script" value={formValues.script} mode="javascript" />
            </div>
            <HStack className="justify-content-between mt-4">
                <div>Model input</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </HStack>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <HStack className="justify-content-between">
                    <div>Prompt</div>
                    <div style={{ maxWidth: 200 }} className="text-truncate" title={formValues.prompt}>
                        {formValues.prompt}
                    </div>
                </HStack>
                {formValues.jsonSchema && (
                    <RowWithPreview label="JSON schema" value={formValues.jsonSchema} mode="json" />
                )}
                {formValues.sampleObject && (
                    <RowWithPreview label="Sample object" value={formValues.sampleObject} mode="json" />
                )}
            </div>
            <HStack className="justify-content-between mt-4">
                <div>Document update</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </HStack>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <RowWithPreview label="Update script" value={formValues.update} mode="javascript" />
            </div>
        </>
    );
}

export function EditGenAiTaskStepSummaryFooter() {
    const dispatch = useAppDispatch();

    return (
        <HStack className="justify-content-between">
            <Button
                variant="secondary"
                className="rounded-pill"
                onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
            >
                <Icon icon="arrow-left" /> Back
            </Button>

            <Button type="submit" variant="primary" className="rounded-pill">
                Save <Icon icon="save" margin="ms-1" />
            </Button>
        </HStack>
    );
}

function RowWithPreview(props: { label: string; value: string; mode: AceEditorMode }) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <div>
            <HStack className="justify-content-between">
                <div>{props.label}</div>
                <Button variant="link" size="xs" onClick={toggleIsOpen} className="pe-0">
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} />
                    {isOpen ? "Collapse" : "Show"}
                </Button>
            </HStack>
            <Collapse in={isOpen} mountOnEnter unmountOnExit>
                <div className="mt-2">
                    <AceEditor mode={props.mode} value={props.value || ""} readOnly />
                </div>
            </Collapse>
        </div>
    );
}

function getBooleanColorClass(value: boolean): `text-${ThemeColor}` {
    return value ? "text-success" : "text-danger";
}

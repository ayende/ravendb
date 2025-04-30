import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { HStack } from "components/common/utilities/HStack";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import classNames from "classnames";
import { AboutViewHeading } from "components/common/AboutView";
import useDialog from "components/common/Dialog";
import Code, { CodeLanguage } from "components/common/Code";

export default function EditGenAiTaskStepSummary() {
    const dispatch = useAppDispatch();
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);

    const formValues = useWatch({ control });

    const dialog = useDialog();

    const showPreview = (value: string, language: CodeLanguage) => {
        dialog({
            title: "Preview",
            message: <Code code={value} language={language} />,
        });
    };

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
                    <div
                        className={classNames(
                            {
                                "text-success": formValues.state === "Enabled",
                            },
                            {
                                "text-danger": formValues.state === "Disabled",
                            }
                        )}
                    >
                        {formValues.state}
                    </div>
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
                        <div
                            className={classNames({
                                "text-success": formValues.isResetScript,
                                "text-danger": !formValues.isResetScript,
                            })}
                        >
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
                <HStack className="justify-content-between">
                    <div>Script</div>
                    <div>
                        <ValueWithPreview
                            value={formValues.script}
                            handleClick={() => showPreview(formValues.script, "javascript")}
                        />
                    </div>
                </HStack>
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
                    <div style={{ maxWidth: 200 }} className="text-truncate">
                        {formValues.prompt}
                    </div>
                </HStack>
                <HStack className="justify-content-between">
                    <div>JSON schema</div>
                    <div>
                        <ValueWithPreview
                            value={formValues.jsonSchema}
                            handleClick={() => showPreview(formValues.jsonSchema, "json")}
                        />
                    </div>
                </HStack>
                <HStack className="justify-content-between">
                    <div>Sample object</div>
                    <div>
                        <ValueWithPreview
                            value={formValues.sampleObject}
                            handleClick={() => showPreview(formValues.sampleObject, "json")}
                        />
                    </div>
                </HStack>
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
                <HStack className="justify-content-between">
                    <div>Update script</div>
                    <div>
                        <ValueWithPreview
                            value={formValues.update}
                            handleClick={() => showPreview(formValues.update, "javascript")}
                        />
                    </div>
                </HStack>
            </div>
            <HStack className="justify-content-between mt-4">
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
        </>
    );
}

function ValueWithPreview(props: { value: string; handleClick: () => void }) {
    if (!props.value) {
        return "Not configured";
    }

    return (
        <>
            Configured{" "}
            <Button variant="link" size="xs" onClick={props.handleClick}>
                <Icon icon="preview" margin="m-0" />
            </Button>
        </>
    );
}

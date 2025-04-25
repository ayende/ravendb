import { FormGroup, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../EditGenAiTask";

export default function EditGenAiTaskNodeField() {
    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const nodes = useAppSelector(clusterSelectors.allNodes);

    const possibleMentors = nodes.filter((x) => x.type === "Member").map((x) => x.nodeTag);

    return (
        <FormGroup>
            {possibleMentors.length === 0 && (
                <RichAlert variant="warning">
                    Currently, the responsible node cannot be selected because there are no nodes available.
                </RichAlert>
            )}
            <FormGroup>
                <FormSwitch control={control} name="isSetResponsibleNode">
                    Set Responsible Node
                </FormSwitch>
            </FormGroup>
            {formValues.isSetResponsibleNode && (
                <>
                    <FormGroup>
                        <FormSelect
                            control={control}
                            name="responsibleNode"
                            options={possibleMentors.map((x) => ({ value: x, label: `Node ${x}` }))}
                        />
                    </FormGroup>
                    {formValues.responsibleNode && (
                        <FormGroup>
                            <FormSwitch
                                control={control}
                                name="isPinResponsibleNode"
                                title="Toggle on to pin selected node"
                            >
                                Pin node
                            </FormSwitch>
                            <RichAlert variant="info">
                                {formValues.isPinResponsibleNode ? (
                                    <>
                                        The selected node is now Pinned to handle this task.
                                        <br />
                                        When this node is down, the task will Not execute as no other node will be
                                        selected to handle the task.
                                        <br />
                                        In case the node is removed from the Database Group, a failover will occur as
                                        the cluster will select another node to handle the task.
                                    </>
                                ) : (
                                    <>
                                        The selected node will be the Preferred Node to handle the task.
                                        <br />
                                        When this node is down, the cluster selects another node from the Database Group
                                        to handle the task.
                                    </>
                                )}
                                <strong>
                                    <br />
                                    This option won&apos;t be respected in case of sharded databases.
                                </strong>
                            </RichAlert>
                        </FormGroup>
                    )}
                </>
            )}
        </FormGroup>
    );
}

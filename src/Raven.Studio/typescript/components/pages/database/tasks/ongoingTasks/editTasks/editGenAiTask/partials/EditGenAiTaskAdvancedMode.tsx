import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Code from "components/common/Code";
import { FormAceEditor } from "components/common/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { HStack } from "components/common/utilities/HStack";
import { useAppUrls } from "components/hooks/useAppUrls";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import FormGroup from "react-bootstrap/FormGroup";
import FormLabel from "react-bootstrap/FormLabel";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import { Icon } from "components/common/Icon";
import router from "plugins/router";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { editGenAiTaskUtils } from "../utils/editGenAiTaskUtils";
import EditGenAiTaskBasicFields from "./fields/EditGenAiTaskBasicFields";
import EditGenAiTaskContextFields from "./fields/EditGenAiTaskContextFields";
import EditGenAiTaskModelFields from "./fields/EditGenAiTaskModelFields";
export default function EditGenAiTaskAdvancedMode() {
    const taskId = useAppSelector(editGenAiTaskSelectors.taskId);
    const sourceView = useAppSelector(editGenAiTaskSelectors.sourceView);

    const { formState, control, setValue, trigger, setError, clearErrors } = useFormContext<EditGenAiTaskFormData>();

    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const formValues = useWatch({ control });

    const { appUrl } = useAppUrls();

    const goBack = () => {
        if (sourceView === "AiTasks") {
            router.navigate(appUrl.forAiTasks(databaseName));
        } else {
            router.navigate(appUrl.forOngoingTasks(databaseName));
        }
    };

    console.log("kalczur errors", formState.errors);

    const asyncRunTest = useAsyncCallback(
        async (mode: "applyUpdateScript" | "createContextObjects" | "sendToModel") => {
            if (!formValues.documentId) {
                setError("documentId", { message: "Please select a Document ID" });
                return;
            } else {
                clearErrors("documentId");
            }

            const isValid = await trigger();

            if (!isValid || !formValues.documentId) {
                return;
            }

            setIsTestResultsOpen(true);

            const applyUpdateScript = mode === "applyUpdateScript";
            const createContextObjects = mode === "createContextObjects";
            const sendToModel = mode === "sendToModel";

            const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
                ApplyUpdateScript: applyUpdateScript,
                CreateContextObjects: createContextObjects,
                Results: formValues.contextOutput ? JSON.parse(formValues.contextOutput) : null,
                SendToModel: sendToModel,
                DocumentId: formValues.documentId,
                IsDelete: false,
                Configuration: editGenAiTaskUtils.mapToDto(formValues, taskId),
            };

            const result = await tasksService.testGenAi(databaseName, dto);

            setValue("contextOutput", JSON.stringify(result.Results, null, 2));
            return result;
        }
    );

    const { value: isTestResultsOpen, setValue: setIsTestResultsOpen } = useBoolean(false);

    return (
        <div>
            <Col md={isTestResultsOpen ? 8 : 12} className="overflow-scroll">
                <HStack className="mb-3 justify-content-between">
                    <HStack gap={2}>
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            icon="save"
                            isSpinning={formState.isSubmitting}
                            disabled={!formState.isDirty}
                        >
                            Save
                        </ButtonWithSpinner>
                        <Button variant="secondary" onClick={goBack}>
                            <Icon icon="cancel" />
                            Cancel
                        </Button>
                    </HStack>
                </HStack>
                <HStack className="justify-content-between align-items-center mt-4">
                    <h3>Basic configuration</h3>
                    <ButtonWithSpinner variant="info rounded-pill" icon="test" isSpinning={false}>
                        Test connection
                    </ButtonWithSpinner>
                </HStack>

                <div className="panel-bg-1 p-4 rounded-2">
                    <EditGenAiTaskBasicFields />
                </div>
                <HStack className="justify-content-between align-items-center mt-2">
                    <h3>Specify task context</h3>
                    <ButtonWithSpinner
                        variant="info rounded-pill"
                        icon="test"
                        isSpinning={false}
                        onClick={() => asyncRunTest.execute("createContextObjects")}
                    >
                        Test task context
                    </ButtonWithSpinner>
                </HStack>
                <div className="panel-bg-1 p-4 rounded-2">
                    <EditGenAiTaskContextFields />
                </div>
                <HStack className="justify-content-between align-items-center mt-2">
                    <h3>Model inputs</h3>
                    <ButtonWithSpinner
                        variant="info rounded-pill"
                        icon="test"
                        isSpinning={false}
                        onClick={() => asyncRunTest.execute("sendToModel")}
                    >
                        Test model
                    </ButtonWithSpinner>
                </HStack>
                <div className="panel-bg-1 p-4 rounded-2">
                    <EditGenAiTaskModelFields />
                </div>
                <HStack className="justify-content-between align-items-center mt-2">
                    <h3>Provide a script for document update</h3>
                    <ButtonWithSpinner
                        variant="info rounded-pill"
                        icon="test"
                        isSpinning={false}
                        onClick={() => asyncRunTest.execute("applyUpdateScript")}
                    >
                        Test script
                    </ButtonWithSpinner>
                </HStack>
                <div className="panel-bg-1 p-4 rounded-2">
                    <FormGroup>
                        <FormLabel>Update script</FormLabel>
                        <FormAceEditor control={control} name="update" mode="javascript" />
                    </FormGroup>
                </div>
            </Col>
            {isTestResultsOpen && (
                <Col md={4} className="panel-bg-1 p-4 border-start">
                    <HStack className="justify-content-between align-items-center">
                        <h3>Test results</h3>
                        <Button variant="link" className="text-reset" onClick={() => setIsTestResultsOpen(false)}>
                            <Icon icon="cancel" />
                        </Button>
                    </HStack>
                    <Tabs defaultActiveKey="context" id="test-results-tabs" className="mb-2" justify>
                        <Tab eventKey="context" title={<span className="text-reset">Context output</span>}>
                            <FormGroup>
                                <FormLabel>Context output</FormLabel>
                                <Code
                                    language="json"
                                    code={JSON.stringify(
                                        asyncRunTest.result.Results.map((result) => result.ContextOutput),
                                        null,
                                        2
                                    )}
                                />
                            </FormGroup>
                        </Tab>
                        {asyncRunTest.result?.Results.some((x) => x.ModelOutput) && (
                            <Tab eventKey="model" title={<span className="text-reset">Model result</span>}>
                                <Code
                                    language="json"
                                    code={JSON.stringify(
                                        asyncRunTest.result.Results.map((result) => result.ModelOutput),
                                        null,
                                        2
                                    )}
                                />
                            </Tab>
                        )}
                        {asyncRunTest.result?.OutputDocument && (
                            <Tab eventKey="update" title={<span className="text-reset">Update script result</span>}>
                                <Code
                                    language="json"
                                    code={JSON.stringify(asyncRunTest.result.OutputDocument, null, 2)}
                                />
                            </Tab>
                        )}
                    </Tabs>
                </Col>
            )}
        </div>
    );
}

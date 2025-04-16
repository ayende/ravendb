import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Button from "react-bootstrap/Button";
import { HStack } from "components/common/utilities/HStack";
import { Icon } from "components/common/Icon";
import * as yup from "yup";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import {
    FormAceEditor,
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSelectCreatable,
    FormSwitch,
} from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import RichAlert from "components/common/RichAlert";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { sortBy } from "common/typeUtils";
import useBoolean from "components/hooks/useBoolean";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import InputGroup from "react-bootstrap/InputGroup";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { tryHandleSubmit } from "components/utils/common";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import Code from "components/common/Code";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import classNames from "classnames";
import documentMetadata from "models/database/documents/documentMetadata";
import { LazyLoad } from "components/common/LazyLoad";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

interface QueryParams {
    taskId: string;
    sourceView: EditAiTaskSourceView;
}

export default function EditGenAiTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const taskId = queryParams.taskId ? parseInt(queryParams.taskId) : null;
    const isNewTask = taskId === null;

    const isEncrypted = useAppSelector(databaseSelectors.activeDatabase)?.isEncrypted ?? false;
    const nodes = useAppSelector(clusterSelectors.allNodes);
    const collectionOptions: SelectOption[] = useAppSelector(collectionsTrackerSelectors.collectionNames).map((x) => ({
        value: x,
        label: x,
    }));

    const possibleMentors = nodes.filter((x) => x.type === "Member").map((x) => x.nodeTag);

    const { tasksService, databasesService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: async () => {
            if (taskId) {
                const dto = await tasksService.getGenAiTaskInfo(databaseName, taskId);
                return getDefaultValues(dto);
            }

            return getDefaultValues(null);
        },
    });

    const { control, handleSubmit, setValue, formState, reset, trigger, setError, clearErrors } = form;

    const formValues = useWatch({ control });

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);
        const connectionStrings = Object.values(result.AiConnectionStrings).map((x) => x.Name);

        return sortBy(connectionStrings, (x) => x.toUpperCase()).map(
            (x) => ({ value: x, label: x }) satisfies SelectOption
        );
    }, []);

    const handleConnectionStringSave = async (connectionName: string) => {
        await asyncGetConnectionStringsOptions.execute();
        setValue("connectionStringName", connectionName, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
        });
        toggleIsNewConnectionStringOpen();
    };

    const { appUrl } = useAppUrls();

    const handleSave: SubmitHandler<FormData> = (data) => {
        return tryHandleSubmit(async () => {
            const scriptsToReset = data.isResetScript ? [data.scriptToReset] : undefined;
            await tasksService.saveGenAiTask(databaseName, mapToDto(data, taskId), scriptsToReset);
            reset(data);
            goBack();
        });
    };

    const goBack = () => {
        if (queryParams.sourceView === "AiTasks") {
            router.navigate(appUrl.forAiTasks(databaseName));
        } else {
            router.navigate(appUrl.forOngoingTasks(databaseName));
        }
    };

    console.log("kalczur errors", formState.errors);

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

    const asyncRunTest = useAsyncCallback(async () => {
        if (!formValues.documentId) {
            setError("documentId", { message: "Please select a Document ID" });
        } else {
            clearErrors("documentId");
        }

        const isValid = await trigger();

        if (!isValid || !formValues.documentId) {
            return;
        }

        const dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript = {
            DocumentId: formValues.documentId,
            IsDelete: false,
            Configuration: mapToDto(formValues, taskId),
        };

        const result = await tasksService.testGenAi(databaseName, dto);
        return result;
    });

    const hasInputErrors = !!formState.errors.script;
    const hasUpdateErrors = !!formState.errors.update;
    const hasModelInputErrors =
        !!formState.errors.prompt || !!formState.errors.jsonSchema || !!formState.errors.sampleObject;

    return (
        <div className="content-padding">
            <AboutViewHeading title={isNewTask ? "New GenAI" : "Edit GenAI"} icon="ai-etl" />

            <form onSubmit={handleSubmit(handleSave)}>
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
                <FormGroup>
                    <FormLabel>Task Name</FormLabel>
                    <FormInput type="text" control={control} name="name" />
                </FormGroup>
                <FormGroup>
                    <FormLabel>Task State</FormLabel>
                    <FormSelect control={control} name="state" options={stateOptions} />
                </FormGroup>
                {isEncrypted && (
                    <div className="vstack gap-2">
                        <RichAlert variant="info">
                            Database <strong>{databaseName}</strong> is encrypted
                        </RichAlert>
                        <FormGroup>
                            <FormSwitch control={control} name="isAllowEtlOnNonEncryptedChannel">
                                Allow task on a non-encrypted communication channel
                            </FormSwitch>
                        </FormGroup>
                    </div>
                )}
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
                                                When this node is down, the task will Not execute as no other node will
                                                be selected to handle the task.
                                                <br />
                                                In case the node is removed from the Database Group, a failover will
                                                occur as the cluster will select another node to handle the task.
                                            </>
                                        ) : (
                                            <>
                                                The selected node will be the Preferred Node to handle the task.
                                                <br />
                                                When this node is down, the cluster selects another node from the
                                                Database Group to handle the task.
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
                <FormGroup>
                    <FormLabel>Connection String</FormLabel>
                    <InputGroup>
                        <FormSelect
                            control={control}
                            name="connectionStringName"
                            options={asyncGetConnectionStringsOptions.result ?? []}
                            isLoading={asyncGetConnectionStringsOptions.loading}
                        />
                        <InputGroup.Text>
                            <ButtonWithSpinner
                                variant="link"
                                className="text-reset px-0"
                                icon="plus"
                                isSpinning={asyncGetConnectionStringsOptions.loading}
                                onClick={toggleIsNewConnectionStringOpen}
                            >
                                Create a new AI connection string
                            </ButtonWithSpinner>
                        </InputGroup.Text>
                        {isNewConnectionStringOpen && (
                            <EditConnectionStrings
                                initialConnection={{ type: "Ai" }}
                                afterSave={handleConnectionStringSave}
                                afterClose={toggleIsNewConnectionStringOpen}
                            />
                        )}
                    </InputGroup>
                </FormGroup>
                {!isNewTask && (
                    <FormGroup>
                        <FormSwitch control={control} name="isResetScript">
                            Regenerate all documents
                        </FormSwitch>
                    </FormGroup>
                )}
                <FormGroup>
                    <FormLabel>Collection Name</FormLabel>
                    <FormSelectCreatable control={control} name="collectionName" options={collectionOptions} />
                </FormGroup>
                <div className="panel-bg-1 p-4 mb-2">
                    <ButtonWithSpinner
                        variant="info"
                        onClick={asyncRunTest.execute}
                        className="mb-2"
                        isSpinning={asyncRunTest.loading}
                        icon="play"
                    >
                        Run test
                    </ButtonWithSpinner>
                    {Object.keys(formState.errors).length > 0 && (
                        <RichAlert variant="warning" className="mb-2">
                            Please fix all errors in the form before running the test.
                        </RichAlert>
                    )}
                    <FormGroup>
                        <FormLabel>Document ID</FormLabel>
                        <FormSelectAutocomplete
                            control={control}
                            name="documentId"
                            options={asyncGetDocumentIdOptions.result ?? []}
                            isLoading={asyncGetDocumentIdOptions.loading}
                        />
                    </FormGroup>

                    <Tabs defaultActiveKey="input" id="gen-ai-tabs" className="mb-2">
                        <Tab
                            eventKey="input"
                            title={
                                <span
                                    className={classNames(
                                        { "text-emphasis": !hasInputErrors },
                                        { "text-danger": hasInputErrors }
                                    )}
                                >
                                    Input / Script
                                </span>
                            }
                        >
                            <Row>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>Input</FormLabel>
                                        <LazyLoad active={asyncGetDocument.loading}>
                                            <Code
                                                language="json"
                                                code={
                                                    asyncGetDocument.result
                                                        ? JSON.stringify(asyncGetDocument.result, null, 2)
                                                        : "Select Document ID"
                                                }
                                            />
                                        </LazyLoad>
                                    </FormGroup>
                                </Col>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>Script</FormLabel>
                                        <FormAceEditor control={control} name="script" mode="javascript" />
                                    </FormGroup>
                                </Col>
                            </Row>
                        </Tab>
                        <Tab
                            eventKey="model-inputs"
                            title={
                                <span
                                    className={classNames(
                                        { "text-emphasis": !hasModelInputErrors },
                                        { "text-danger": hasModelInputErrors }
                                    )}
                                >
                                    Model inputs
                                </span>
                            }
                        >
                            <FormGroup>
                                <FormLabel>Prompt</FormLabel>
                                <FormAceEditor control={control} name="prompt" mode="plain_text" />
                            </FormGroup>
                            <Row>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>Sample Object</FormLabel>
                                        <FormAceEditor control={control} name="sampleObject" mode="json" />
                                    </FormGroup>
                                </Col>
                                <Col>
                                    <FormGroup>
                                        <FormLabel>JSON Schema</FormLabel>
                                        <FormAceEditor control={control} name="jsonSchema" mode="json" />
                                    </FormGroup>
                                </Col>
                            </Row>
                        </Tab>
                        <Tab
                            eventKey="result-from-model"
                            title={
                                <span
                                    className={classNames(
                                        { "text-emphasis": !!asyncRunTest.result },
                                        { "text-muted": !asyncRunTest.result }
                                    )}
                                >
                                    Result from the model
                                </span>
                            }
                            disabled={!asyncRunTest.result}
                        >
                            {asyncRunTest.result && (
                                <Code language="json" code={JSON.stringify(asyncRunTest.result.Results, null, 2)} />
                            )}
                        </Tab>
                        <Tab
                            eventKey="update"
                            title={
                                <span
                                    className={classNames(
                                        { "text-emphasis": !hasUpdateErrors },
                                        { "text-danger": hasUpdateErrors }
                                    )}
                                >
                                    Update
                                </span>
                            }
                        >
                            <FormGroup>
                                <FormAceEditor control={control} name="update" mode="javascript" />
                            </FormGroup>
                        </Tab>
                        <Tab
                            eventKey="output"
                            title={
                                <span
                                    className={classNames(
                                        { "text-emphasis": !!asyncRunTest.result },
                                        { "text-muted": !asyncRunTest.result }
                                    )}
                                >
                                    Output
                                </span>
                            }
                            disabled={!asyncRunTest.result}
                        >
                            {asyncRunTest.result && (
                                <Code
                                    language="json"
                                    code={JSON.stringify(asyncRunTest.result.OutputDocument, null, 2)}
                                />
                            )}
                        </Tab>
                    </Tabs>
                </div>
            </form>
        </div>
    );
}

const stateOptions: SelectOption<OngoingTaskState>[] = (["Enabled", "Disabled"] satisfies OngoingTaskState[]).map(
    (x) => ({
        label: x,
        value: x,
    })
);

const getDefaultValues = (dto: Raven.Client.Documents.Operations.OngoingTasks.GenAi): FormData => {
    if (!dto) {
        return {
            name: "",
            state: "Enabled",
            isSetResponsibleNode: false,
            responsibleNode: null,
            isPinResponsibleNode: false,
            connectionStringName: "",
            isAllowEtlOnNonEncryptedChannel: false,
            collectionName: "",
            prompt: "",
            jsonSchema: "",
            sampleObject: "",
            update: "",
            isResetScript: false,
            scriptToReset: null,
            script: "",
            documentId: "",
        };
    }

    return {
        name: dto.Configuration.Name,
        state: dto.TaskState,
        isSetResponsibleNode: dto.MentorNode != null,
        responsibleNode: dto.MentorNode ?? null,
        isPinResponsibleNode: dto.PinToMentorNode,
        connectionStringName: dto.ConnectionStringName,
        isAllowEtlOnNonEncryptedChannel: dto.Configuration.AllowEtlOnNonEncryptedChannel,
        collectionName: dto.Configuration.Collection,
        prompt: dto.Configuration.Prompt ?? "",
        jsonSchema: dto.Configuration.JsonSchema ?? "",
        sampleObject: dto.Configuration.SampleObject ?? "",
        update: dto.Configuration.Update ?? "",
        isResetScript: true,
        scriptToReset: dto.Configuration.Transforms?.[0].Name ?? null,
        script: dto.Configuration.GenAiTransformation?.Script ?? "",
        documentId: "",
    };
};

const mapToDto = (data: FormData, taskId: number): GenAiConfiguration => {
    return {
        TaskId: taskId,
        Name: data.name,
        EtlType: "GenAi",
        ConnectionStringName: data.connectionStringName,
        AllowEtlOnNonEncryptedChannel: data.isAllowEtlOnNonEncryptedChannel,
        Disabled: data.state === "Disabled",
        MentorNode: data.isSetResponsibleNode ? data.responsibleNode : undefined,
        PinToMentorNode: data.isSetResponsibleNode && data.isPinResponsibleNode,
        Transforms: null,
        Collection: data.collectionName,
        Prompt: data.prompt,
        JsonSchema: data.jsonSchema,
        SampleObject: data.sampleObject,
        Update: data.update,
        GenAiTransformation: {
            Script: data.script,
        },
    };
};

const schema = yup.object({
    name: yup.string().required(),
    state: yup.string<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup.string().nullable(),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().required(),
    isAllowEtlOnNonEncryptedChannel: yup.boolean(),
    collectionName: yup.string().required(),
    prompt: yup.string().required(),
    jsonSchema: yup.string(),
    sampleObject: yup.string(),
    update: yup.string().required(),
    isResetScript: yup.boolean(),
    scriptToReset: yup.string().nullable(),
    script: yup.string().required(),
    // For testing
    documentId: yup.string(),
});

type FormData = yup.InferType<typeof schema>;

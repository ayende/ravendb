import { EditGenAiTaskFormData } from "./editGenAiTaskValidation";

const getDefaultValues = (dto: Raven.Client.Documents.Operations.OngoingTasks.GenAi): EditGenAiTaskFormData => {
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
            schemaProvider: null,
            jsonSchema: "",
            sampleObject: "",
            update: "",
            isResetScript: false,
            scriptToReset: null,
            script: "",
            documentId: "",
            playgroundContexts: [],
            playgroundModelOutputs: [],
            playgroundDocument: "",
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
        schemaProvider: dto.Configuration.JsonSchema ? "jsonSchema" : "sampleObject",
        jsonSchema: dto.Configuration.JsonSchema ?? "",
        sampleObject: dto.Configuration.SampleObject ?? "",
        update: dto.Configuration.Update ?? "",
        isResetScript: true,
        scriptToReset: dto.Configuration.Transforms?.[0].Name ?? null,
        script: dto.Configuration.GenAiTransformation?.Script ?? "",
        documentId: "",
        playgroundContexts: [],
        playgroundModelOutputs: [],
        playgroundDocument: "",
    };
};

const mapToDto = (
    data: EditGenAiTaskFormData,
    taskId: number
): Raven.Client.Documents.Operations.AI.GenAiConfiguration => {
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
        Identifier: undefined,
    };
};

export const editGenAiTaskUtils = {
    getDefaultValues,
    mapToDto,
};
